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
    using System.Collections.Generic;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.WindowsAzure.Storage.Table.Protocol;

    internal sealed class StellarQueryExecutor<TResult, TElement> : IQueryExecutor<TResult, TElement>
    {
        public TableQuerySegment<TResult> ExecuteQuerySegmented(
            TableQuery<TElement> query,
            TableContinuationToken token, 
            CloudTableClient client, 
            CloudTable table,
            TableRequestOptions requestOptions, 
            OperationContext operationContext)
        {
            // GetAwaiter().GetResult() unwraps the aggregate exception and throws original exception
            return this.ExecuteQuerySegmentedInternalAsync(query, token, client, table, requestOptions, operationContext).GetAwaiter().GetResult();
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
            return new WrappedAsyncResult<TableQuerySegment<TResult>>(
                t => ExecuteQuerySegmentedInternalAsync(query, token, client, table, requestOptions, operationContext),
                this,
                callback, 
                state);
        }

        public TableQuerySegment<TResult> EndExecute(IAsyncResult asyncResult)
        {
            try
            {
                return ((WrappedAsyncResult<TableQuerySegment<TResult>>)asyncResult).Result;
            }
            catch (AggregateException ex)
            {
                // Preserve the original call stack
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }

        internal async Task<TableQuerySegment<TResult>> QueryCollectionsAsync<TResult>(
            TableQuery<TElement> query,
            TableContinuationToken token,
            CloudTableClient client,
            CloudTable table,
            TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            if (string.IsNullOrWhiteSpace(query.FilterString))
            {
                throw new NotSupportedException("FilterText is not supported for query enumeration");
            }

            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = query.TakeCount,
                RequestContinuation = token != null ? token.NextRowKey : null
            };

            FeedResponse<DocumentCollection> collectionQueryResponse
                = await client.DocumentClient
                            .CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(StellarConstants.TableDatabaseName), feedOptions)
                            .AsDocumentQuery()
                            .ExecuteNextAsync<DocumentCollection>();

            List<TResult> resultCollection = new List<TResult>();
            foreach (DocumentCollection documentCollection in collectionQueryResponse)
            {
                DynamicTableEntity tableEntity = new DynamicTableEntity();
                tableEntity.Properties.Add(TableConstants.TableName, new EntityProperty(documentCollection.Id));
                resultCollection.Add((TResult)(object)tableEntity);
            }

            TableQuerySegment<TResult> result = new TableQuerySegment<TResult>(resultCollection);
            if (!string.IsNullOrEmpty(collectionQueryResponse.ResponseContinuation))
            {
                result.ContinuationToken = new TableContinuationToken() { NextRowKey = collectionQueryResponse.ResponseContinuation };
            }

            return result;
        }

        private async Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedInternalAsync(
            TableQuery<TElement> query,
            TableContinuationToken token,
            CloudTableClient client,
            CloudTable table,
            TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            if (string.Equals(table.Name, TableConstants.TableServiceTablesName, StringComparison.OrdinalIgnoreCase))
            {
                return await this.QueryCollectionsAsync<TResult>(query, token, client, table, requestOptions, operationContext);
            }

            throw new NotImplementedException();
        }
    }
}
