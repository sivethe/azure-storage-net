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
    using System.Linq;
    using System.Text;

    internal static class Util
    {
        private static DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public static double GetMilliSecondsSinceEpoch(DateTime dateTime)
        {
            return (Int64)((DateTime)dateTime.ToUniversalTime() - Epoch).TotalMilliseconds;
        }

        public static DateTime GetDateTimeFromEpoch(double milliSecondsSinceEpoch)
        {
            return Epoch.AddMilliseconds(milliSecondsSinceEpoch);
        }

        public static string BytesToString(byte[] bytes)
        {
            if (bytes == null)
            {
                throw new ArgumentNullException("bytes");
            }

            StringBuilder stringBuilder = new StringBuilder();
            for (int index = 0; index < bytes.Length; index++)
            {
                stringBuilder.Append((char)bytes[index]);
            }

            return stringBuilder.ToString();
        }

        public static byte[] StringToBytes(string bytesString)
        {
            if (bytesString == null)
            {
                throw new ArgumentNullException("bytesString");
            }

            byte[] bytes = new byte[bytesString.Length];
            for (int index = 0; index < bytesString.Length; index++)
            {
                bytes[index] = (byte)bytesString[index];
            }

            return bytes;
        }

        public static bool IsIdentifierCharacter(char c)
        {
            return c == '_' || char.IsLetterOrDigit(c);
        }

        public static int GetHexValue(char c)
        {
            if ((c >= '0') && (c <= '9')) return (c - '0');
            if ((c >= 'a') && (c <= 'f')) return (c - 'a' + 10);
            if ((c >= 'A') && (c <= 'F')) return (c - 'A' + 10);

            return -1;
        }

        public static bool IsHexCharacter(char c)
        {
            if (c >= '0' && c <= '9') return true;
            if (c >= 'a' && c <= 'f') return true;
            if (c >= 'A' && c <= 'F') return true;

            return false;
        }

        public static string GetQuotedString(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return "\"" + Util.GetEscapedString(value) + "\"";
        }

        public static string GetEscapedString(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            if (value.All(c => !Util.IsEscapedCharacter(c)))
            {
                return value;
            }

            var stringBuilder = new StringBuilder(value.Length);

            foreach (char c in value)
            {
                switch (c)
                {
                    case '"':
                        stringBuilder.Append("\\\"");
                        break;
                    case '\\':
                        stringBuilder.Append("\\\\");
                        break;
                    case '\b':
                        stringBuilder.Append("\\b");
                        break;
                    case '\f':
                        stringBuilder.Append("\\f");
                        break;
                    case '\n':
                        stringBuilder.Append("\\n");
                        break;
                    case '\r':
                        stringBuilder.Append("\\r");
                        break;
                    case '\t':
                        stringBuilder.Append("\\t");
                        break;
                    default:
                        switch (char.GetUnicodeCategory(c))
                        {
                            case UnicodeCategory.UppercaseLetter:
                            case UnicodeCategory.LowercaseLetter:
                            case UnicodeCategory.TitlecaseLetter:
                            case UnicodeCategory.OtherLetter:
                            case UnicodeCategory.DecimalDigitNumber:
                            case UnicodeCategory.LetterNumber:
                            case UnicodeCategory.OtherNumber:
                            case UnicodeCategory.SpaceSeparator:
                            case UnicodeCategory.ConnectorPunctuation:
                            case UnicodeCategory.DashPunctuation:
                            case UnicodeCategory.OpenPunctuation:
                            case UnicodeCategory.ClosePunctuation:
                            case UnicodeCategory.InitialQuotePunctuation:
                            case UnicodeCategory.FinalQuotePunctuation:
                            case UnicodeCategory.OtherPunctuation:
                            case UnicodeCategory.MathSymbol:
                            case UnicodeCategory.CurrencySymbol:
                            case UnicodeCategory.ModifierSymbol:
                            case UnicodeCategory.OtherSymbol:
                                stringBuilder.Append(c);
                                break;
                            default:
                                stringBuilder.AppendFormat("\\u{0:x4}", (int)c);
                                break;
                        }
                        break;
                }
            }

            return stringBuilder.ToString();
        }

        private static bool IsEscapedCharacter(char c)
        {
            switch (c)
            {
                case '"':
                case '\\':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t':
                    return true;

                default:
                    switch (char.GetUnicodeCategory(c))
                    {
                        case UnicodeCategory.UppercaseLetter:
                        case UnicodeCategory.LowercaseLetter:
                        case UnicodeCategory.TitlecaseLetter:
                        case UnicodeCategory.OtherLetter:
                        case UnicodeCategory.DecimalDigitNumber:
                        case UnicodeCategory.LetterNumber:
                        case UnicodeCategory.OtherNumber:
                        case UnicodeCategory.SpaceSeparator:
                        case UnicodeCategory.ConnectorPunctuation:
                        case UnicodeCategory.DashPunctuation:
                        case UnicodeCategory.OpenPunctuation:
                        case UnicodeCategory.ClosePunctuation:
                        case UnicodeCategory.InitialQuotePunctuation:
                        case UnicodeCategory.FinalQuotePunctuation:
                        case UnicodeCategory.OtherPunctuation:
                        case UnicodeCategory.MathSymbol:
                        case UnicodeCategory.CurrencySymbol:
                        case UnicodeCategory.ModifierSymbol:
                        case UnicodeCategory.OtherSymbol:
                            return false;

                        default:
                            return true;
                    }
            }
        }
    }
}
