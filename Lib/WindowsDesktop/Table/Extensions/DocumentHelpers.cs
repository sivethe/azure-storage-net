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

namespace Microsoft.WindowsAzure.Storage.Table.Extensions
{
    using System;
    using System.Net;
    using Microsoft.Azure.Documents;
    using Microsoft.WindowsAzure.Storage.Table.Protocol;

    internal static class DocumentHelpers
    {
        public static StorageException ToStorageException(this Exception exception, TableOperation tableOperation)
        {
            DocumentClientException docException = exception as DocumentClientException;
            RequestResult reqResult = new RequestResult();

            if (docException == null)
            {
                if (exception is TimeoutException)
                {
                    return StorageException.TranslateException(exception, new RequestResult());
                }

                reqResult.HttpStatusCode = (int)500;
                reqResult.HttpStatusMessage = TableErrorCodeMessageStrings.InternalServerError;
                return new StorageException(reqResult, exception.Message, exception);
            }
            else
            {
                reqResult.HttpStatusMessage = docException.Error.Code;
                reqResult.HttpStatusCode = (int)docException.StatusCode;

                reqResult.ExtendedErrorInformation = new StorageExtendedErrorInformation();
                reqResult.ExtendedErrorInformation.ErrorCode = docException.Error.Code;
                reqResult.ExtendedErrorInformation.ErrorMessage = docException.Message;

                switch (docException.StatusCode)
                {
                    case HttpStatusCode.Conflict:
                        if (tableOperation != null && tableOperation.IsTableEntity)
                        {
                            reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.TableAlreadyExists;
                            reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.TableAlreadyExistsMessage;
                        }
                        else
                        {
                            reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.EntityAlreadyExists;
                            reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.EntityAlreadyExistsMessage;
                        }
                        break;

                    case HttpStatusCode.NotFound:
                        if (tableOperation != null && tableOperation.IsTableEntity)
                        {
                            reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.ResourceNotFound;
                            reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.ResourceNotFoundMessage;
                        }
                        else
                        {
                            reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.EntityNotFound;
                            reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.EntityNotFoundMessage;
                        }
                        break;

                    case HttpStatusCode.PreconditionFailed:
                        reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.UpdateConditionNotSatisfied;
                        reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.UpdateConditionNotSatisfiedMessage;
                        break;

                    case HttpStatusCode.RequestEntityTooLarge:
                        reqResult.ExtendedErrorInformation.ErrorCode = TableErrorCodeStrings.EntityTooLarge;
                        reqResult.ExtendedErrorInformation.ErrorMessage = TableErrorCodeMessageStrings.EntityTooLargeMessage;
                        break;
                }

                return new StorageException(reqResult, reqResult.ExtendedErrorInformation.ErrorMessage, docException);
            }
        }
    }
}
