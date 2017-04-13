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

namespace Microsoft.WindowsAzure.Storage.Table
{
    using System;
    using Microsoft.WindowsAzure.Storage.Core.Executor;

    internal sealed class TableQueryExecutor<TElement> : IQueryExecutor<TElement>
    {
        public TableQuerySegment<TElement> ExecuteQuerySegmented(
            TableQuery<TElement> query, 
            TableContinuationToken token, 
            CloudTableClient client,
            CloudTable table, 
            TableRequestOptions requestOptions, 
            OperationContext operationContext)
        {
            RESTCommand<TableQuerySegment<TElement>> cmdToExecute = query.QueryImpl<TElement, TElement>(query, token, client, table, EntityUtilities.ResolveEntityByType<TElement>, requestOptions);
            return Executor.ExecuteSync(cmdToExecute, requestOptions.RetryPolicy, operationContext);
        }

        public ICancellableAsyncResult BeginExecuteQuerySegmented(
            TableQuery<TElement> query, 
            TableContinuationToken token,
            CloudTableClient client, 
            CloudTable table, 
            TableRequestOptions requestOptions, 
            OperationContext operationContext,
            AsyncCallback callback, 
            object state)
        {
            return Executor.BeginExecuteAsync(
                                          query.QueryImpl<TElement, TElement>(query, token, client, table, EntityUtilities.ResolveEntityByType<TElement>, requestOptions),
                                          requestOptions.RetryPolicy,
                                          operationContext,
                                          callback,
                                          state);
        }

        public TableQuerySegment<TElement> EndExecute(IAsyncResult asyncResult)
        {
            return Executor.EndExecuteAsync<TableQuerySegment<TElement>>(asyncResult);
        }
    }
}
