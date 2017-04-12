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
    using System.Globalization;
    using System.IO;
    using System.Linq;

    internal sealed class TableEntityWriter : ITableEntityWriter
    {
        private readonly TextWriter textWriter;
        private TableEntityWriterContext context;
        private TableEntityWriterState state;
        private string elemantName;
        private bool disposed;

        public TableEntityWriter(TextWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            this.textWriter = writer;
            this.context = new TableEntityWriterContext();
            this.state = TableEntityWriterState.Initial;
        }

        #region ITableEntityWriter
        public void Close()
        {
            if (this.state != TableEntityWriterState.CLosed)
            {
                this.Flush();
                this.state = TableEntityWriterState.CLosed;
            }
        }

        public void Flush()
        {
            this.ThrowIfDisposed();
            this.textWriter.Flush();
        }

        public void Start()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteStart", TableEntityWriterState.Initial);

            this.textWriter.Write('{');
            this.state = TableEntityWriterState.Name;
        }

        public void End()
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteStart", TableEntityWriterState.Name);

            this.textWriter.Write('}');
            this.state = TableEntityWriterState.Done;
        }

        public void WriteName(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteName", TableEntityWriterState.Name);

            this.elemantName = name;
            this.state = TableEntityWriterState.Value;
        }

        public void WriteValue(EntityProperty value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteValue", TableEntityWriterState.Value);

            this.WriteNameAux(this.elemantName);
            if (value.IsNull)
            {
                this.WriteNull(value.PropertyType);
            }
            else
            {
                switch (value.PropertyType)
                {
                    case EdmType.String:
                        this.WriteString(value.StringValue);
                        break;
                    case EdmType.Binary:
                        this.WriteBinary(value.BinaryValue);
                        break;
                    case EdmType.Boolean:
                        this.WriteBoolean(value.BooleanValue);
                        break;
                    case EdmType.DateTime:
                        this.WriteDateTime(value.DateTime);
                        break;
                    case EdmType.Double:
                        this.WriteDouble(value.DoubleValue);
                        break;
                    case EdmType.Guid:
                        this.WriteGuid(value.GuidValue);
                        break;
                    case EdmType.Int32:
                        this.WriteInt32(value.Int32Value);
                        break;
                    case EdmType.Int64:
                        this.WriteInt64(value.Int64Value);
                        break;
                    default:
                        throw new InvalidOperationException("Unexpected EdmType");
                }
            }

            this.UpdateState();
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            if (!this.disposed)
            {
                this.Close();
                this.disposed = true;
            }
        }
        #endregion

        #region Private methods
        private void WriteString(string value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteString", TableEntityWriterState.Value);

            this.WriteValue<string>(EdmType.String, Util.GetQuotedString(value));
        }

        private void WriteBinary(byte[] value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteBinary", TableEntityWriterState.Value);

            this.WriteValue<string>(EdmType.Binary, Util.GetQuotedString(Util.BytesToString(value)));
        }

        private void WriteBoolean(bool? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteBoolean", TableEntityWriterState.Value);

            this.WriteValue<string>(EdmType.Boolean, value.Value ? "true" : "false");
        }

        private void WriteDateTime(DateTime? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteDateTime", TableEntityWriterState.Value);

            this.WriteValue<double>(EdmType.DateTime, Util.GetMilliSecondsSinceEpoch(value.Value));
        }

        private void WriteDouble(double? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteDouble", TableEntityWriterState.Value);

            this.WriteValue<double>(EdmType.Double, value.Value);
        }

        private void WriteGuid(Guid? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteGuid", TableEntityWriterState.Value);

            this.WriteValue<string>(EdmType.Guid, Util.GetQuotedString(value.ToString()));
        }

        private void WriteInt32(int? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteInt32", TableEntityWriterState.Value);

            this.WriteValue<Int32>(EdmType.Int32, value.Value);
        }

        private void WriteInt64(long? value)
        {
            this.ThrowIfDisposed();
            this.ThrowIfInvalidState("WriteInt64", TableEntityWriterState.Value);

            this.WriteValue<string>(EdmType.Int64, Util.GetQuotedString(value.Value.ToString("D20", CultureInfo.InvariantCulture)));
        }

        private void WriteNameAux(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            if (this.context.HasElements)
            {
                this.textWriter.Write(", ");
            }

            this.textWriter.Write(Util.GetQuotedString(name));
            this.textWriter.Write(": ");
            this.context.HasElements = true;
        }

        private void UpdateState()
        {
            if (this.state == TableEntityWriterState.Name)
            {
                this.state = TableEntityWriterState.Value;
            }
            else
            {
                this.state = TableEntityWriterState.Name;
            }
        }

        private void WriteNull(EdmType type)
        {
            this.textWriter.Write('{');

            // Write type marker
            this.textWriter.Write("\"{0}\": {1}", "$t", (int)type);
            this.textWriter.Write(", ");
            this.textWriter.Write("\"{0}\": {1}", "$v", "null");

            this.textWriter.Write('}');
        }

        private void WriteValue<TValue>(EdmType type, TValue value)
        {
            if (value == null)
            {
                this.WriteNull(type);
            }
            else
            {
                this.textWriter.Write('{');

                // Write type marker
                this.textWriter.Write("\"{0}\": {1}", "$t", (int)type);
                this.textWriter.Write(", ");
                this.textWriter.Write("\"{0}\": {1}", "$v", value);

                this.textWriter.Write('}');
            }
        }
        #endregion

        #region Helpers
        private void ThrowIfDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("TableEntityWriter");
            }
        }

        private void ThrowIfInvalidState(string methodName, params TableEntityWriterState[] validStates)
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

        #endregion
    }
}
