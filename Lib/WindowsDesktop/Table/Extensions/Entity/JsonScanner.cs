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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal sealed class JsonScanner
    {
        private sealed class BufferReader
        {
            private readonly string buffer;
            private int atomStartIndex;
            private int atomEndIndex;

            public BufferReader(string buffer)
            {
                if (buffer == null)
                {
                    throw new ArgumentNullException("buffer");
                }

                this.buffer = buffer;
            }

            public int AtomLength { get { return this.atomEndIndex - this.atomStartIndex; } }

            public bool IsEof { get { return this.atomEndIndex >= this.buffer.Length; } }

            public bool CheckNext(Func<char, bool> predicate)
            {
                Debug.Assert(predicate != null);

                return !this.IsEof && predicate(this.buffer[this.atomEndIndex]);
            }

            public bool ReadNext(out char c)
            {
                if (!this.IsEof)
                {
                    c = this.buffer[this.atomEndIndex++];
                    return true;
                }

                c = '\0';
                return false;
            }

            public bool ReadNextIfEquals(char c)
            {
                if (!this.IsEof && c == this.buffer[this.atomEndIndex])
                {
                    this.atomEndIndex++;
                    return true;
                }

                return false;
            }

            public bool ReadNextIfEquals(char c1, char c2)
            {
                if (!this.IsEof && (c1 == this.buffer[this.atomEndIndex] || c2 == this.buffer[this.atomEndIndex]))
                {
                    this.atomEndIndex++;
                    return true;
                }

                return false;
            }

            public int AdvanceWhile(Func<char, bool> predicate, bool condition)
            {
                Debug.Assert(predicate != null);

                int startIndex = this.atomEndIndex;
                while (this.atomEndIndex < this.buffer.Length)
                {
                    if (predicate(this.buffer[this.atomEndIndex]) != condition)
                    {
                        break;
                    }

                    this.atomEndIndex++;
                }

                return this.atomEndIndex - startIndex;
            }

            public bool UndoRead()
            {
                if (this.atomEndIndex > this.atomStartIndex)
                {
                    this.atomEndIndex--;
                    return true;
                }

                Debug.Assert(this.atomStartIndex == this.atomEndIndex);

                return false;
            }

            public void StartNewAtom()
            {
                this.atomStartIndex = this.atomEndIndex;
            }

            public bool TryParseAtomAsDecimal(out double number)
            {
                string atomText = this.GetAtomText();
                if (double.TryParse(atomText, NumberStyles.Any, CultureInfo.InvariantCulture, out number))
                {
                    return true;
                }

                number = 0;
                return false;
            }

            public bool TryParseAtomAsHex(out double number)
            {
                string atomText = this.GetAtomText();
                if (string.IsNullOrEmpty(atomText)
                    || atomText.Length < 2
                    || atomText[0] != '0'
                    || (atomText[1] != 'x' && atomText[1] != 'X'))
                {
                    Debug.Assert(false);
                }

                long longNumber;
                if (long.TryParse(atomText.Substring(2), System.Globalization.NumberStyles.HexNumber, CultureInfo.InvariantCulture, out longNumber))
                {
                    number = longNumber;
                    return true;
                }

                number = 0;
                return false;
            }

            public string GetAtomText()
            {
                // endIndex can be equal to buffer.Length if this is the last atom
                if (this.atomEndIndex > this.buffer.Length)
                {
                    throw new InvalidOperationException();
                }

                return this.buffer.Substring(this.atomStartIndex, this.AtomLength);
            }
        }

        private enum ScanState
        {
            Error,
            HasValue,
            Initial
        }

        private enum ScanStringState
        {
            Continue,
            Done,
            Error,
            ReadEscapedCharacter,
            ReadUnicodeCharacter,
        }

        private struct UnicodeCharacter
        {
            public int Value;
            public int DigitCount;
        }

        private readonly BufferReader reader;
        private ScanState state;
        private JsonToken currentToken;

        public JsonScanner(string buffer)
        {
            this.reader = new BufferReader(buffer);
            this.state = ScanState.Initial;
        }

        public bool IsEof { get { return this.reader.IsEof; } }

        public JsonToken GetCurrentToken()
        {
            if (this.state != ScanState.HasValue)
            {
                throw new InvalidOperationException();
            }

            return this.currentToken;
        }

        public bool ScanNext()
        {
            if (this.state != ScanState.Initial && this.state != ScanState.HasValue)
            {
                throw new InvalidOperationException();
            }

            this.state = this.ScanNextPrivate();
            if (this.state == ScanState.HasValue)
            {
                return true;
            }

            return false;
        }

        private ScanState ScanNextPrivate()
        {
            this.reader.StartNewAtom();

            char current;
            if (!this.reader.ReadNext(out current))
            {
                return ScanState.Error;
            }

            switch (current)
            {
                case '\'':
                case '"':
                    return this.ScanDelimitedString(current);

                case '.':
                    if (this.reader.CheckNext(char.IsDigit))
                    {
                        this.reader.UndoRead();
                        return this.ScanDecimal();
                    }

                    return ScanState.Error;

                case '-':
                    this.reader.UndoRead();
                    return this.ScanDecimal();

                case ',':
                    this.currentToken = JsonToken.Comma;
                    return ScanState.HasValue;

                case ':':
                    this.currentToken = JsonToken.Colon;
                    return ScanState.HasValue;

                case '{':
                    this.currentToken = JsonToken.BeginObject;
                    return ScanState.HasValue;

                case '}':
                    this.currentToken = JsonToken.EndObject;
                    return ScanState.HasValue;

                case '[':
                    this.currentToken = JsonToken.BeginArray;
                    return ScanState.HasValue;

                case ']':
                    this.currentToken = JsonToken.EndArray;
                    return ScanState.HasValue;

                default:
                    break;
            }

            if (char.IsWhiteSpace(current))
            {
                this.reader.AdvanceWhile(char.IsWhiteSpace, true);
                return this.ScanNextPrivate();
            }

            if (char.IsDigit(current))
            {
                if (current == '0' && this.reader.ReadNextIfEquals('x', 'X'))
                {
                    return this.ScanHexNumber();
                }

                this.reader.UndoRead();
                return this.ScanDecimal();
            }

            return this.ScanUnquotedString();
        }

        private ScanState ScanDelimitedString(char quotationChar)
        {
            Debug.Assert(quotationChar == '"' || quotationChar == '\'' || quotationChar == '/');

            char current;
            StringBuilder stringBuilder = new StringBuilder();
            ScanStringState scanState = ScanStringState.Continue;
            UnicodeCharacter unicodeChar = new UnicodeCharacter();

            while (this.reader.ReadNext(out current))
            {
                switch (scanState)
                {
                    case ScanStringState.Continue:
                        if (current == quotationChar)
                        {
                            scanState = ScanStringState.Done;
                        }
                        else if (current == '\\')
                        {
                            scanState = ScanStringState.ReadEscapedCharacter;
                        }
                        break;
                    case ScanStringState.ReadEscapedCharacter:
                        if (current == 'u')
                        {
                            unicodeChar = new UnicodeCharacter();
                            scanState = ScanStringState.ReadUnicodeCharacter;
                        }
                        else
                        {
                            switch (current)
                            {
                                case '\'':
                                    current = '\'';
                                    break;
                                case '"':
                                    current = '"';
                                    break;
                                case '\\':
                                    current = '\\';
                                    break;
                                case '/':
                                    current = '/';
                                    break;
                                case 'b':
                                    current = '\b';
                                    break;
                                case 'f':
                                    current = '\f';
                                    break;
                                case 'n':
                                    current = '\n';
                                    break;
                                case 'r':
                                    current = '\r';
                                    break;
                                case 't':
                                    current = '\t';
                                    break;
                                default:
                                    break;
                            }

                            scanState = ScanStringState.Continue;
                        }
                        break;
                    case ScanStringState.ReadUnicodeCharacter:
                        if (Util.IsHexCharacter(current))
                        {
                            unicodeChar.Value = unicodeChar.Value << 4;
                            unicodeChar.Value += Util.GetHexValue(current);
                            unicodeChar.DigitCount++;

                            if (unicodeChar.DigitCount == 4)
                            {
                                current = (char)unicodeChar.Value;
                                scanState = ScanStringState.Continue;
                            }
                        }
                        else
                        {
                            scanState = ScanStringState.Error;
                        }
                        break;
                    default:
                        Debug.Assert(false);
                        break;
                }

                if (scanState == ScanStringState.Continue)
                {
                    stringBuilder.Append(current);
                }
                else if (scanState == ScanStringState.Done || scanState == ScanStringState.Error)
                {
                    break;
                }
            }

            if (scanState == ScanStringState.Done)
            {
                this.currentToken = JsonToken.StringToken(stringBuilder.ToString(), this.reader.GetAtomText());
                return ScanState.HasValue;
            }

            return ScanState.Error;
        }

        private ScanState ScanUnquotedString()
        {
            Debug.Assert(this.reader.AtomLength == 1);

            this.reader.AdvanceWhile(Util.IsIdentifierCharacter, true);
            string stringToken = this.reader.GetAtomText();

            switch (stringToken)
            {
                case "Infinity":
                    this.currentToken = JsonToken.Infinity;
                    return ScanState.HasValue;

                case "NaN":
                    this.currentToken = JsonToken.NaN;
                    return ScanState.HasValue;

                case "true":
                    this.currentToken = JsonToken.True;
                    return ScanState.HasValue;

                case "false":
                    this.currentToken = JsonToken.False;
                    return ScanState.HasValue;

                case "null":
                    this.currentToken = JsonToken.Null;
                    return ScanState.HasValue;
            }

            return ScanState.Error;
        }

        private ScanState ScanDecimal()
        {
            Debug.Assert(this.reader.AtomLength == 0);

            this.reader.ReadNextIfEquals('-');
            this.reader.AdvanceWhile(char.IsDigit, true);

            if (this.reader.ReadNextIfEquals('.'))
            {
                this.reader.AdvanceWhile(char.IsDigit, true);
            }

            if (this.reader.ReadNextIfEquals('e', 'E'))
            {
                this.reader.ReadNextIfEquals('+', '-');

                if (this.reader.AdvanceWhile(char.IsDigit, true) <= 0)
                {
                    return ScanState.Error;
                }
            }

            if (this.reader.AdvanceWhile(Util.IsIdentifierCharacter, true) > 0)
            {
                return ScanState.Error;
            }

            double number;
            if (!this.reader.TryParseAtomAsDecimal(out number))
            {
                return ScanState.Error;
            }

            this.currentToken = JsonToken.NumberToken(number, this.reader.GetAtomText());
            return ScanState.HasValue;
        }

        private ScanState ScanHexNumber()
        {
            Debug.Assert(this.reader.AtomLength == 2);

            this.reader.AdvanceWhile(Util.IsHexCharacter, true);

            if (this.reader.AdvanceWhile(Util.IsIdentifierCharacter, true) > 0)
            {
                return ScanState.Error;
            }

            double number;
            if (!this.reader.TryParseAtomAsHex(out number))
            {
                return ScanState.Error;
            }

            this.currentToken = JsonToken.NumberToken(number, this.reader.GetAtomText());
            return ScanState.HasValue;
        }
    }
}
