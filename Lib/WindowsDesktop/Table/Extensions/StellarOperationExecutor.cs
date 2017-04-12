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
    using System.Configuration;
    using System.Net;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.WindowsAzure.Storage.Table.Extension;
    using Microsoft.WindowsAzure.Storage.Table.Protocol;

    internal sealed class StellarOperationExecutor : IOperationExecutor
    {
        public TableResult Execute(
            TableOperation operation,
            CloudTableClient client, 
            CloudTable table, 
            TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            // GetAwaiter().GetResult() unwraps the aggregate exception and throws original exception
            return this.ExecuteAsync(operation, client, table, requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public ICancellableAsyncResult BeginExecute(
            TableOperation operation,
            CloudTableClient client, 
            CloudTable table, 
            TableRequestOptions requestOptions,
            OperationContext operationContext, 
            AsyncCallback callback, 
            object state)
        {
            return new WrappedAsyncResult<TableResult, IOperationExecutor>(
                t => ExecuteAsync(operation, client, table, requestOptions, operationContext), 
                this,
                callback, 
                state);
        }

        public TableResult EndExecute(IAsyncResult asyncResult)
        {
            try
            {
                return ((WrappedAsyncResult<TableResult, IOperationExecutor>)asyncResult).Result;
            }
            catch (AggregateException ex)
            {
                // Preserve the original call stack
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }

        private async Task<TableResult> ExecuteAsync(
            TableOperation operation,
            CloudTableClient client,
            CloudTable table,
            TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            try
            {
                switch (operation.OperationType)
                {
                    case TableOperationType.Insert:
                        return await this.HandleInsertAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.InsertOrMerge:
                        return await this.HandleInsertOrMergeAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.Merge:
                        return await this.HandleMergeAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.Delete:
                        return await this.HandleDeleteAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.InsertOrReplace:
                        return await this.HandleUpsertAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.Replace:
                        return await this.HandleReplaceAsync(operation, client, table, requestOptions, operationContext);

                    case TableOperationType.Retrieve:
                        return await this.HandleReadAsync(operation, client, table, requestOptions, operationContext);

                    default:
                        throw new NotSupportedException();
                }
            }
            catch (Exception ex)
            {
                throw EntityHelpers.GetTableResultFromException(ex, operation);
            }
        }

        private async Task<TableResult> HandleInsertOrMergeAsync(
            TableOperation operation, 
            CloudTableClient client, 
            CloudTable table, 
            TableRequestOptions options,
            OperationContext context)
        {
            Document document = EntityHelpers.GetDocumentFromEntity(operation.Entity, context, options);

            try
            {
                ResourceResponse<Document> response = await client.DocumentClient.CreateDocumentAsync(table.GetCollectionUri(), document);
                return this.GetTableResultFromResponse(operation, response, context, options);
            }
            catch (DocumentClientException exception)
            {
                if (exception.StatusCode != HttpStatusCode.Conflict)
                {
                    throw;
                }
            }

            return await this.HandleMergeAsync(operation, client, table, options, context);
        }

        private async Task<TableResult> HandleInsertAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options, OperationContext context)
        {
            if (operation.IsTableEntity)
            {
                await client.DocumentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = StellarConstants.TableDatabaseName });

                string collectionName = ((DynamicTableEntity)operation.Entity).Properties[TableConstants.TableName].StringValue;

                RequestOptions requestOptions = null;
                string collectionRUConfig = ConfigurationManager.AppSettings["DocumentDbCollectionRU"];
                int collectionRU = 0;
                if (!string.IsNullOrEmpty(collectionRUConfig) && int.TryParse(collectionRUConfig, out collectionRU))
                {
                    requestOptions = new RequestOptions { OfferThroughput = collectionRU };
                }

                ResourceResponse<DocumentCollection> response
                    = await client.DocumentClient.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(StellarConstants.TableDatabaseName),
                        new DocumentCollection
                        {
                            Id = collectionName,
                            PartitionKey = new PartitionKeyDefinition() { Paths = { "/" + StellarConstants.PartitionKeyPropertyName } },

                        }, requestOptions);
                return EntityHelpers.GetTableResultFromResponse(response, context);
            }
            else
            {
                Document document = EntityHelpers.GetDocumentFromEntity(operation.Entity, context, options);

                ResourceResponse<Document> response = await client.DocumentClient.CreateDocumentAsync(table.GetCollectionUri(), document);

                return this.GetTableResultFromResponse(operation, response, context, options);
            }
        }

        private async Task<TableResult> HandleUpsertAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options,OperationContext context)
        {
            Document document = EntityHelpers.GetDocumentFromEntity(operation.Entity, context, options);
            ResourceResponse<Document> response = await client.DocumentClient.UpsertDocumentAsync(table.GetCollectionUri(), document);
            return this.GetTableResultFromResponse(operation, response, context, options);
        }

        private async Task<TableResult> HandleReplaceAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options, OperationContext context)
        {
            Document document = EntityHelpers.GetDocumentFromEntity(operation.Entity, context, options);
            RequestOptions requestOptions = null;
            Uri documentUri = this.GetDocumentUri(operation, table, out requestOptions);
            if (!string.IsNullOrEmpty(operation.Entity.ETag))
            {
                requestOptions.AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition { Type = AccessConditionType.IfMatch, Condition = operation.Entity.ETag };
            }

            ResourceResponse<Document> response = await client.DocumentClient.ReplaceDocumentAsync(documentUri, document, requestOptions);
            return this.GetTableResultFromResponse(operation, response, context, options);
        }

        private async Task<TableResult> HandleDeleteAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options, OperationContext context)
        {
            if (operation.IsTableEntity)
            {
                string collectionName = ((DynamicTableEntity)operation.Entity).Properties[TableConstants.TableName].StringValue;
                Uri documentCollectionUri = UriFactory.CreateDocumentCollectionUri(StellarConstants.TableDatabaseName, collectionName);
                ResourceResponse<DocumentCollection> response = await client.DocumentClient.DeleteDocumentCollectionAsync(documentCollectionUri);
                return EntityHelpers.GetTableResultFromResponse(response, context);
            }
            else
            {
                RequestOptions requestOptions;
                Uri documentUri = this.GetDocumentUri(operation, table, out requestOptions);
                ResourceResponse<Document> response = await client.DocumentClient.DeleteDocumentAsync(documentUri, requestOptions);
                return this.GetTableResultFromResponse(operation, response, context, options);
            }
        }

        private async Task<TableResult> HandleReadAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options, OperationContext context)
        {
            try
            {
                if (operation.IsTableEntity)
                {
                    //TODO:  Why do we get the collection name from the Entity instead of the table
                    string collectionName = ((DynamicTableEntity)operation.Entity).Properties[TableConstants.TableName].StringValue;
                    Uri documentCollectionUri = UriFactory.CreateDocumentCollectionUri(StellarConstants.TableDatabaseName, collectionName);
                    ResourceResponse<DocumentCollection> response = await client.DocumentClient.ReadDocumentCollectionAsync(documentCollectionUri);

                    return EntityHelpers.GetTableResultFromResponse(response, context);
                }
                else
                {
                    RequestOptions requestOptions;
                    Uri documentUri = this.GetDocumentUri(operation, table, out requestOptions);

                    ResourceResponse<Document> response = await client.DocumentClient.ReadDocumentAsync(documentUri, requestOptions);
                    return this.GetTableResultFromResponse(operation, response, context, options);
                }
            }
            catch (DocumentClientException exception)
            {
                if (exception.StatusCode == HttpStatusCode.NotFound)
                {
                    return new TableResult
                    {
                        HttpStatusCode = (int)HttpStatusCode.NotFound
                    };
                }

                throw;
            }
        }

        private async Task<TableResult> HandleMergeAsync(TableOperation operation, CloudTableClient client, CloudTable table, TableRequestOptions options, OperationContext context)
        {
            ResourceResponse<Document> readResponse = await client.DocumentClient.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(StellarConstants.TableDatabaseName, table.Name, operation.Entity.RowKey),
                    new RequestOptions { PartitionKey = new PartitionKey(operation.Entity.PartitionKey) });

            Document mergedDocument = readResponse.Resource;
            Document newDocument = EntityHelpers.GetDocumentFromEntity(operation.Entity, context, options);

            foreach (KeyValuePair<string, EntityProperty> property in operation.Entity.WriteEntity(context))
            {
                mergedDocument.SetPropertyValue(property.Key, newDocument.GetPropertyValue<object>(property.Key));
            }

            ResourceResponse<Document> updateResponse = await client.DocumentClient.ReplaceDocumentAsync(
                mergedDocument,
                new RequestOptions
                {
                    AccessCondition =
                        new Microsoft.Azure.Documents.Client.AccessCondition
                        {
                            Type = AccessConditionType.IfMatch,
                            Condition = readResponse.Resource.ETag
                        }
                });

            return this.GetTableResultFromResponse(operation, updateResponse, context, options);
        }

        private TableResult GetTableResultFromResponse(TableOperation operation, ResourceResponse<Document> response, OperationContext context, TableRequestOptions options)
        {
            TableResult result = new TableResult();
            result.Etag = response.ResponseHeaders["ETag"];
            result.HttpStatusCode = (int)response.StatusCode;

            if (operation.OperationType != TableOperationType.Retrieve)
            {
                if (operation.OperationType != TableOperationType.Delete)
                {
                    result.Result = operation.Entity;

                    ITableEntity tableEntity = result.Result as ITableEntity;
                    if (tableEntity != null)
                    {
                        tableEntity.ETag = result.Etag;
                        tableEntity.Timestamp = response.Resource.Timestamp;
                    }
                }
            }
            else
            {
                if (response.Resource != null)
                {
                    if (operation.RetrieveResolver == null)
                    {
                        result.Result = EntityHelpers.GetEntityFromDocument(response.Resource, context);
                    }
                    else
                    {
                        IDictionary<string, EntityProperty> properties = EntityHelpers.GetEntityFromDocument(response.Resource, context).WriteEntity(context);

                        if (options != null && options.EncryptionPolicy != null)
                        {
                            properties = EntityHelpers.DecryptEntityProperties(properties, options, response);
                        }

                        result.Result = operation.RetrieveResolver(response.Resource.GetPropertyValue<string>(StellarConstants.TableDatabaseName),
                            response.Resource.Id, response.Resource.Timestamp, properties,
                            response.Resource.ETag);
                    }
                }
            }

            return result;
        }

        private Uri GetDocumentUri(TableOperation operation, CloudTable table, out RequestOptions requestOptions)
        {
            requestOptions = new RequestOptions { PartitionKey = new PartitionKey(operation.PartitionKey) };

            return UriFactory.CreateDocumentUri(StellarConstants.TableDatabaseName, table.Name, operation.RowKey);
        }
    }
}
