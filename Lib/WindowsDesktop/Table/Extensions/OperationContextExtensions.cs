// -----------------------------------------------------------------------------------------
// <copyright file="DocumentHelpers.cs" company="Microsoft">
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

using System.Net;

namespace Microsoft.WindowsAzure.Storage.Table.Extensions
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    internal static class OperationContextExtensions
    {
        internal static RequestResult ToRequestResult<T>(this ResourceResponse<T> response)
            where T : Resource, new()
        {
            return new RequestResult
            {
                HttpStatusCode = (int) HttpStatusCode.Accepted,
                HttpStatusMessage = null,
                ServiceRequestID = null,
                Etag = null,
                RequestDate = null,
            };
        }

        internal static RequestResult ToRequestResult<T>(this FeedResponse<T> response)
        {
            return new RequestResult
            {
                HttpStatusCode = (int)HttpStatusCode.Accepted,
                HttpStatusMessage = null,
                ServiceRequestID = null,
                Etag = null,
                RequestDate = null,
            };
        }
    }
}
