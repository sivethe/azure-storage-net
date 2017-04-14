//-----------------------------------------------------------------------
// <copyright file="TableErrorCodeMessageStrings.cs" company="Microsoft">
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

namespace Microsoft.WindowsAzure.Storage.Table.Extensions
{
    /// <summary>
    /// Provides error code message strings that are specific to the Microsoft Azure Table service.
    /// </summary>    
    internal static class TableErrorCodeMessageStrings
    {
        public static readonly string TableAlreadyExistsMessage = "The specified table already exists.";

        public static readonly string TableNotFoundMessage = "The specified table was not found.";

        public static readonly string EntityNotFoundMessage = "The specified entity was not found.";

        public static readonly string EntityAlreadyExistsMessage = "The specified entity already exists.";

        public static readonly string UpdateConditionNotSatisfiedMessage = "The specified update condition was not satisfied.";

        public static readonly string EntityTooLargeMessage = "The entity is larger than the maximum size permitted.";

        public static readonly string InternalServerError = "Server encountered an internal error.Please try again after some time.";

        public static readonly string ResourceNotFoundMessage = "The specified resource does not exist.";
    }
}