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

    internal enum JsonTokenType
    {
        BeginArray,
        BeginObject,
        Boolean,
        Colon,
        Comma,
        EndArray,
        EndObject,
        Null,
        Number,
        String,
    }

    internal class JsonToken
    {
        private static readonly JsonToken BeginObjectToken = new JsonToken(JsonTokenType.BeginObject, "{");
        private static readonly JsonToken EndObjectToken = new JsonToken(JsonTokenType.EndObject, "}");
        private static readonly JsonToken BeginArrayToken = new JsonToken(JsonTokenType.BeginArray, "[");
        private static readonly JsonToken EndArrayToken = new JsonToken(JsonTokenType.EndArray, "]");
        private static readonly JsonToken ColonToken = new JsonToken(JsonTokenType.Colon, ":");
        private static readonly JsonToken CommaToken = new JsonToken(JsonTokenType.Comma, ",");
        private static readonly JsonToken NullToken = new JsonToken(JsonTokenType.Null, "null");

        private static readonly NumberJsonToken NaNToken = new NumberJsonToken(double.NaN, "NaN");
        private static readonly NumberJsonToken InfinityToken = new NumberJsonToken(double.PositiveInfinity, "Infinity");

        private static readonly BooleanJsonToken TrueToken = new BooleanJsonToken(true, "true");
        private static readonly BooleanJsonToken FalseToken = new BooleanJsonToken(false, "false");

        private readonly JsonTokenType type;
        private readonly string lexeme;

        protected JsonToken(JsonTokenType type, string lexeme)
        {
            this.type = type;
            this.lexeme = lexeme;
        }

        public JsonTokenType Type
        {
            get { return this.type; }
        }

        public string Lexeme
        {
            get { return this.lexeme; }
        }

        public virtual double GetDoubleValue()
        {
            throw new InvalidCastException();
        }

        public virtual string GetStringValue()
        {
            throw new InvalidCastException();
        }

        public virtual bool GetBooleanValue()
        {
            throw new InvalidCastException();
        }

        // JsonToken constants
        public static JsonToken BeginObject
        {
            get { return JsonToken.BeginObjectToken; }
        }

        public static JsonToken EndObject
        {
            get { return JsonToken.EndObjectToken; }
        }

        public static JsonToken BeginArray
        {
            get { return JsonToken.BeginArrayToken; }
        }

        public static JsonToken EndArray
        {
            get { return JsonToken.EndArrayToken; }
        }

        public static JsonToken Colon
        {
            get { return JsonToken.ColonToken; }
        }

        public static JsonToken Comma
        {
            get { return JsonToken.CommaToken; }
        }

        public static JsonToken Null
        {
            get { return JsonToken.NullToken; }
        }

        // Boolean
        public static JsonToken True
        {
            get { return JsonToken.TrueToken; }
        }

        public static JsonToken False
        {
            get { return JsonToken.FalseToken; }
        }

        // Double
        public static JsonToken NaN
        {
            get { return JsonToken.NaNToken; }
        }

        public static JsonToken Infinity
        {
            get { return JsonToken.InfinityToken; }
        }

        public static JsonToken NumberToken(double value, string lexeme)
        {
            return new NumberJsonToken(value, lexeme);
        }

        // String
        public static JsonToken StringToken(string value, string lexeme)
        {
            return new StringJsonToken(value, lexeme);
        }

        private sealed class BooleanJsonToken : JsonToken
        {
            private readonly bool value;

            public BooleanJsonToken(bool value, string lexeme)
                : base(JsonTokenType.Boolean, lexeme)
            {
                this.value = value;
            }

            public override bool GetBooleanValue()
            {
                return this.value;
            }
        }

        private sealed class NumberJsonToken : JsonToken
        {
            private readonly double value;

            public NumberJsonToken(double value, string lexeme)
                : base(JsonTokenType.Number, lexeme)
            {
                if (lexeme == null)
                {
                    throw new ArgumentNullException("lexeme");
                }

                this.value = value;
            }

            public override double GetDoubleValue()
            {
                return this.value;
            }
        }

        private sealed class StringJsonToken : JsonToken
        {
            private readonly string value;

            public StringJsonToken(string value, string lexeme)
                : base(JsonTokenType.String, lexeme)
            {
                if (value == null)
                {
                    throw new ArgumentNullException("value");
                }

                if (lexeme == null)
                {
                    throw new ArgumentNullException("lexeme");
                }

                this.value = value;
            }

            public override string GetStringValue()
            {
                return this.value;
            }
        }
    }
}
