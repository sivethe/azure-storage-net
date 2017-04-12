// -----------------------------------------------------------------------------------------
// <copyright file="TableQuery.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
// -----------------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.Table.Extension
{
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Diagnostics;

    internal sealed class TableEntityReader : ITableEntityReader
    {
        private static List<int> edmTypeValues;

        private readonly JsonScanner scanner;
        private TableEntityReaderState state;
        private JsonToken currentToken;
        private JsonToken pushedToken;
        private EntityProperty currentValue;
        private string currentName;
        private EdmType currentEdmType;
        private bool disposed;

        static TableEntityReader()
        {
            TableEntityReader.edmTypeValues = new List<int>();
            foreach (var type in Enum.GetValues(typeof(EdmType)))
            {
                TableEntityReader.edmTypeValues.Add((int)type);
            }
        }

        public TableEntityReader(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                throw new ArgumentException("json");
            }

            this.scanner = new JsonScanner(json);
            this.state = TableEntityReaderState.Initial;
        }

        #region ITableEntityReader
        public void Start()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("Start", TableEntityReaderState.Initial);

            this.Expect(JsonTokenType.BeginObject);
        }

        public void End()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("End", TableEntityReaderState.Done);
        }

        public bool MoveNext()
        {
            this.ThrowIfDisposed();
            if (this.state == TableEntityReaderState.Done)
            {
                return false;
            }

            JsonToken nameToken = this.PopToken();
            if (nameToken.Type == JsonTokenType.EndObject)
            {
                this.state = TableEntityReaderState.Done;
                return false;
            }

            if (nameToken.Type == JsonTokenType.String)
            {
                string name = nameToken.GetStringValue();
                if (this.IsDocumentDBProperty(name))
                {
                    // just skip it
                    this.Expect(JsonTokenType.Colon);
                    this.PopToken();
                    this.TryReadComma();
                    return this.MoveNext();
                }

                this.currentName = nameToken.GetStringValue();
            }
            else
            {
                this.ThrowFormatException("Expecting a name but found '{0}'", nameToken.Lexeme);
            }

            this.Expect(JsonTokenType.Colon);
            JsonToken valueToken = this.PopToken();
            if (valueToken.Type != JsonTokenType.BeginObject)
            {
                this.ThrowFormatException("Value is expected to be an object instead it was '{0}'.", valueToken.Type);
            }

            this.currentEdmType = this.ParseEdmType();
            this.TryReadComma();
            this.state = TableEntityReaderState.HasValue;

            return true;
        }

        public string ReadCurrentName()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("ReadCurrentName", TableEntityReaderState.HasValue);

            return this.currentName;
        }

        public EntityProperty ReadCurrentValue()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("ReadCurrentValue", TableEntityReaderState.HasValue);

            return this.currentValue;
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            if (!this.disposed)
            {
                this.disposed = true;
            }
        }
        #endregion

        #region Private methods
        bool IsDocumentDBProperty(string name)
        {
            return name == "_rid"
                || name == "_self"
                || name == "_etag"
                || name == "_attachments"
                || name == "_ts"
                || name == "id"
                || name == "partitionKey";
        }

        private EdmType ParseEdmType()
        {
            JsonToken typeMarker = this.PopToken();
            if (typeMarker.Type != JsonTokenType.String || typeMarker.GetStringValue() != "$t")
            {
                this.ThrowFormatException("Expecting type marker but found '{0}'", typeMarker.Type.ToString());
            }

            this.Expect(JsonTokenType.Colon);
            this.Expect(JsonTokenType.Number);

            double typeValue = this.currentToken.GetDoubleValue();
            if (typeValue % 1 != 0 || !TableEntityReader.edmTypeValues.Contains((int)typeValue))
            {
                this.ThrowFormatException("Invalid Edm type: {0}", typeValue);
            }
            EdmType edmType = (EdmType)typeValue;

            this.Expect(JsonTokenType.Comma);
            this.Expect("$v");
            this.Expect(JsonTokenType.Colon);

            this.currentToken = this.PopToken();
            if (currentToken.Type == JsonTokenType.Null)
            {
                this.currentValue = this.CreateEntityPropertyWithNullValue(edmType);
            }
            else
            {
                switch (edmType)
                {
                    case EdmType.String:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.String);
                        this.currentValue = new EntityProperty(this.currentToken.GetStringValue());
                        break;
                    case EdmType.Binary:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.String);
                        this.currentValue = new EntityProperty(Util.StringToBytes(this.currentToken.GetStringValue()));
                        break;
                    case EdmType.Boolean:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.Boolean);
                        this.currentValue = new EntityProperty(this.currentToken.GetBooleanValue());
                        break;
                    case EdmType.DateTime:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.Number);
                        this.currentValue = new EntityProperty(Util.GetDateTimeFromEpoch(this.currentToken.GetDoubleValue()));
                        break;
                    case EdmType.Double:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.Number);
                        this.currentValue = new EntityProperty(this.currentToken.GetDoubleValue());
                        break;
                    case EdmType.Guid:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.String);
                        this.currentValue = new EntityProperty(Guid.Parse(this.currentToken.GetStringValue()));
                        break;
                    case EdmType.Int32:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.Number);
                        this.currentValue = new EntityProperty((Int32)this.currentToken.GetDoubleValue());
                        break;
                    case EdmType.Int64:
                        this.EnsureMatchingTypes(this.currentToken.Type, JsonTokenType.String);
                        long value = long.Parse(this.currentToken.GetStringValue(), CultureInfo.InvariantCulture);
                        this.currentValue = new EntityProperty(value);
                        break;
                    default:
                        throw new InvalidOperationException("Unexpected EdmType");
                }
            }

            this.Expect(JsonTokenType.EndObject);
            return edmType;
        }

        private EntityProperty CreateEntityPropertyWithNullValue(EdmType edmType)
        {
            switch (edmType)
            {
                case EdmType.String:
                    return EntityProperty.GeneratePropertyForString(null);
                case EdmType.Binary:
                    return EntityProperty.GeneratePropertyForByteArray(null);
                case EdmType.Boolean:
                    return EntityProperty.GeneratePropertyForBool(null);
                case EdmType.DateTime:
                    return EntityProperty.GeneratePropertyForDateTimeOffset(null);
                case EdmType.Double:
                    return EntityProperty.GeneratePropertyForDouble(null);
                case EdmType.Guid:
                    return EntityProperty.GeneratePropertyForGuid(null);
                case EdmType.Int32:
                    return EntityProperty.GeneratePropertyForInt(null);
                case EdmType.Int64:
                    return EntityProperty.GeneratePropertyForLong(null);
                default:
                    throw new InvalidOperationException("Unexpected EdmType");
            }
        }
        #endregion

        #region Helpers

        private void Expect(JsonTokenType type)
        {
            JsonToken token = this.PopToken();
            if (token.Type != type)
            {
                this.ThrowFormatException("Expecting type {0} but found {1}", type, token.Type);
            }

            this.currentToken = token;
        }

        private void EnsureMatchingTypes(JsonTokenType type1, JsonTokenType type2)
        {
            if (type1 != type2)
            {
                this.ThrowFormatException("type should be {0} but found {1}", type1, type2);
            }
        }

        private void Expect(string stringToken)
        {
            this.Expect(JsonTokenType.String);
            if (this.currentToken.GetStringValue() != stringToken)
            {
                this.ThrowFormatException("Expecting token {0} but found {1}", stringToken, this.currentToken.GetStringValue());
            }
        }

        private void PushToken(JsonToken token)
        {
            Debug.Assert(this.pushedToken == null);
            this.pushedToken = token;
        }

        private JsonToken PopToken()
        {
            if (this.pushedToken != null)
            {
                JsonToken token = this.pushedToken;
                this.pushedToken = null;
                return token;
            }
            else
            {
                if (this.scanner.ScanNext())
                {
                    return this.scanner.GetCurrentToken();
                }

                throw new Exception("Scanner failed.");
            }
        }

        private bool TryReadComma()
        {
            JsonToken commaToken = this.PopToken();
            if (commaToken.Type == JsonTokenType.Comma)
            {
                return true;
            }

            this.PushToken(commaToken);
            return false;
        }

        private void ThrowIfDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("TableEntityReader");
            }
        }

        private void ThrowIfInvalidState(string methodName, params TableEntityReaderState[] validStates)
        {
            foreach (var validState in validStates)
            {
                if (this.state == validState)
                {
                    return;
                }
            }

            var validStatesString = string.Join(" or ", validStates.Select(s => s.ToString()).ToArray());
            throw new InvalidOperationException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} can only be called when state is {1}, actual state is {2}",
                    methodName,
                    validStatesString,
                    this.state.ToString()));
        }

        private void ThrowIfInvalidType(string methodName, EdmType expectedType)
        {
            if (this.currentEdmType == expectedType)
            {
                return;
            }

            throw new InvalidOperationException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} expects current type to be {1}, actual type is {2}",
                    methodName,
                    expectedType.ToString(),
                    this.currentEdmType.ToString()));
        }

        private void ThrowFormatException(string format, params object[] args)
        {
            throw new FormatException(string.Format(CultureInfo.InvariantCulture, format, args));
        }
        #endregion
    }
}
