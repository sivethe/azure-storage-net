// -----------------------------------------------------------------------------------------
// <copyright file="TableBatchOperation.cs" company="Microsoft">
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
    using System.Linq;
    using System.Net;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.WindowsAzure.Storage.Core;
    using Microsoft.WindowsAzure.Storage.Table.Extension;
    using Newtonsoft.Json.Linq;

    internal sealed class StellarBatchExecutor : IOperationExecutor<IList<TableResult>, TableBatchOperation>
    {
        private const string BulkInsertOrMergeOrUpdate = "BulkInsertOrMergeOrUpdate";

        public IList<TableResult> Execute(
            TableBatchOperation batchOperation,
            CloudTableClient client,
            CloudTable table,
            TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            // GetAwaiter().GetResult() unwraps the aggregate exception and throws original exception
            return this.ExecuteAsync(batchOperation, client, table, requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public ICancellableAsyncResult BeginExecute(
            TableBatchOperation batchOperation,
            CloudTableClient client, 
            CloudTable table,
            TableRequestOptions requestOptions, 
            OperationContext operationContext, 
            AsyncCallback callback, 
            object state)
        {
            return new WrappedAsyncResult<IList<TableResult>>(
                t => ExecuteAsync(batchOperation, client, table, requestOptions, operationContext),
                this,
                callback,
                state);
        }

        public IList<TableResult> EndExecute(IAsyncResult asyncResult)
        {
            try
            {
                return ((WrappedAsyncResult<IList<TableResult>>)asyncResult).Result;
            }
            catch (AggregateException ex)
            {
                // Preserve the original call stack
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }

        private async Task<StoredProcedure> GetOrCreateStoredProcedureAsync(CloudTable table, string id, string script)
        {
            try
            {
                return await table.ServiceClient.DocumentClient.ReadStoredProcedureAsync(
                    UriFactory.CreateStoredProcedureUri(StellarConstants.TableDatabaseName, table.Name, id));
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode != HttpStatusCode.NotFound)
                {
                    throw;
                }
            }

            try
            {
                await table.ServiceClient.DocumentClient.CreateStoredProcedureAsync(
                    table.GetCollectionUri(), new StoredProcedure { Id = id, Body = script });
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode != HttpStatusCode.Conflict)
                {
                    throw;
                }
            }

            return (await table.ServiceClient.DocumentClient.ReadStoredProcedureAsync(
                UriFactory.CreateStoredProcedureUri(StellarConstants.TableDatabaseName, table.Name, id))).Resource;
        }

        private TableResult GetTableResultFromDocument(TableOperation operation, Document response, OperationContext context)
        {
            TableResult result = new TableResult();
            result.Etag = response.ETag;
            result.HttpStatusCode = GetSuccessStatusCodeFromOperationType(operation.OperationType);

            if (operation.OperationType != TableOperationType.Retrieve)
            {
                operation.Entity.ETag = response.ETag;
                result.Result = operation.Entity;
            }
            else
            {
                if (operation.RetrieveResolver != null)
                {
                    result.Result = operation.RetrieveResolver(response.GetPropertyValue<string>(StellarConstants.PartitionKeyPropertyName),
                                    response.Id, response.Timestamp, EntityHelpers.GetEntityFromDocument(response, context).WriteEntity(context),
                                    response.ETag);
                }
                else
                {
                    result.Result = EntityHelpers.GetEntityFromDocument(response, context);
                }
            }

            return result;
        }

        private int GetSuccessStatusCodeFromOperationType(TableOperationType operationType)
        {
            // per https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Table-Service-REST-API
            switch (operationType)
            {
                case TableOperationType.Insert:
                    return (int)HttpStatusCode.Created;
                case TableOperationType.Delete:
                case TableOperationType.InsertOrMerge:
                case TableOperationType.InsertOrReplace:
                case TableOperationType.Merge:
                case TableOperationType.Replace:
                    return (int)HttpStatusCode.NoContent;
                case TableOperationType.Retrieve:
                    return (int)HttpStatusCode.OK;
                case TableOperationType.RotateEncryptionKey:
                default:
                    return (int)HttpStatusCode.OK;
            }
        }

        private async Task<IList<TableResult>> ExecuteAsync(
            TableBatchOperation batch,
            CloudTableClient client,
            CloudTable table,
            TableRequestOptions requestOptions, 
            OperationContext operationContext)
        {
            if (batch.Count == 0)
            {
                throw new InvalidOperationException(SR.EmptyBatchOperation);
            }

            if (batch.Count > 100)
            {
                throw new InvalidOperationException(SR.BatchExceededMaximumNumberOfOperations);
            }

            try
            {
                string script = StellarBatchExecutor.MakeCreateDocumentsScript();
                StoredProcedure sproc = await this.GetOrCreateStoredProcedureAsync(table, StellarBatchExecutor.BulkInsertOrMergeOrUpdate, script);
                List<Document> javaScriptParams1 = new List<Document>();
                List<TableOperationType> javaScriptParams2 = new List<TableOperationType>();
                List<string> javaScriptParams3 = new List<string>();
                string partitionKey = batch.FirstOrDefault().PartitionKey;
                foreach (TableOperation operation in batch)
                {
                    Document document = null;
                    if (operation.OperationType == TableOperationType.Retrieve)
                    {
                        document = this.GetDocumentWithPartitionAndRowKey(operation.RetrievePartitionKey, operation.RetrieveRowKey);
                    }
                    else
                    {
                        document = EntityHelpers.GetDocumentFromEntity(operation.Entity, operationContext, requestOptions);
                    }

                    javaScriptParams1.Add(document);
                    javaScriptParams2.Add(operation.OperationType);
                    javaScriptParams3.Add(operation.Entity == null ? "" : operation.Entity.ETag);
                }

                RequestOptions docdbRequestOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                StoredProcedureResponse<string> response =
                    await table.ServiceClient.DocumentClient.ExecuteStoredProcedureAsync<string>(sproc.SelfLink, docdbRequestOptions,
                    javaScriptParams1.ToArray(), javaScriptParams2.ToArray(), javaScriptParams3.ToArray());
                JArray jArray = JArray.Parse(response.Response);

                List<TableResult> tableResults = new List<TableResult>();

                for (int i = 0; i < jArray.Count; i++)
                {
                    tableResults.Add(GetTableResultFromDocument(batch[i], jArray[i].ToObject<Document>(), operationContext));
                }

                return tableResults;
            }
            catch (Exception ex)
            {
                throw EntityHelpers.GetTableResultFromException(ex);
            }
        }

        private Document GetDocumentWithPartitionAndRowKey(string partitionKey, string rowKey)
        {
            Document document = new Document();
            document.Id = rowKey;
            document.SetPropertyValue(StellarConstants.PartitionKeyPropertyName, partitionKey);

            return document;
        }

        public static string MakeCreateDocumentsScript()
        {
            const string scriptTemplate = @"
                function sproc(documents, operations, etags) {
                    if (!documents || !operations || !etags) 
                    {
                        throw new Error('One of the input arrays (operations, operationType, etags is undefined or null.');
                    }

                    if(documents.Count != operations.Count || documents.Count != etags.Count) throw new Error('Operations, operationtype, etags count are inconsistent.');

                    var links = new Array();
                    var i = 0;
                    var isOperationLastStep = false;
                    var isReadAttempted = false;
                    var isInsertAttempted = false;
                    var docResource;
                    var collection = getContext().getCollection();
                    var collectionLink = collection.getAltLink();
    
                    ProcessBatch();

                    function ProcessBatch() {
                        var document = documents[i];
                        var operation = operations[i];
                        var etag = etags[i];

                        if(operation == '0') // Insert
                        {
                            isOperationLastStep = true;
                            if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                                failedToEnqueueOperation();
                            } 
                        }
                        else if(operation == '1') // delete
                        {
                            let documentLink = `${collectionLink}/docs/${document.id}`;
                            isOperationLastStep = true;
                            if(!collection.deleteDocument(documentLink, {etag: etag}, onDocumentActionCompletion)) {
                                failedToEnqueueOperation();
                            } 
                        }
                        else if(operation == '2') // replace
                        {
                            let documentLink = `${collectionLink}/docs/${document.id}`;
                            isOperationLastStep = true;
                            if(!collection.replaceDocument(documentLink, document, {etag: etag}, onDocumentActionCompletion)) {
                                failedToEnqueueOperation();
                            } 
                        }
                        else if(operation == '3') // merge?
                        {
                            let documentLink = `${collectionLink}/docs/${document.id}`;
                            if(!isReadAttempted)
                            {
                                if (!collection.readDocument(documentLink, {}, onReadCompletion)) {
                                    failedToEnqueueOperation();
                                }
                            }
                            else
                            {
                                for (var propName of Object.getOwnPropertyNames(document))
                                {
                                    docResource[propName] = document[propName];
                                }

                                isOperationLastStep = true;
                                if(!collection.replaceDocument(documentLink, docResource, 
                                   {etag: etag}, onDocumentActionCompletion)) {
                                    failedToEnqueueOperation();
                                } 
                            }

                        }
                        else if(operation == '4') // insertreplace - Upsert (create and/or replace)
                        {
                            if(!isInsertAttempted)
                            {
                                if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                                    failedToEnqueueOperation();
                                }
                            }
                            else
                            {
                                let documentLink = `${collectionLink}/docs/${document.id}`;
                                isOperationLastStep = true;
                                if(!collection.replaceDocument(documentLink, document, {}, onDocumentActionCompletion)) {
                                    failedToEnqueueOperation();
                                }
                            }
                        }
                        else if(operation == '5') // insertMerge - Read, Merge, Update
                        {
                            let documentLink = `${collectionLink}/docs/${document.id}`;
                            if(!isInsertAttempted)
                            {
                                if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                                    failedToEnqueueOperation();
                                }
                            }
                            else if(!isReadAttempted)
                            {
                                if (!collection.readDocument(documentLink, {}, onReadCompletion)) {
                                    failedToEnqueueOperation();
                                }
                            }
                            else
                            {
                                for (var propName of Object.getOwnPropertyNames(document))
                                {
                                    docResource[propName] = document[propName];
                                }
                
                                isOperationLastStep = true;
                                if(!collection.replaceDocument(documentLink, docResource, 
                                   {}, onDocumentActionCompletion)) {
                                    failedToEnqueueOperation();
                                } 
                            }
                        }
                        else if(operation == '6') // retrieve
                        {
                            let documentLink = `${collectionLink}/docs/${document.id}`;
                            isOperationLastStep = true;
                            if (!collection.readDocument(documentLink, {}, onDocumentActionCompletion)) {
                                    failedToEnqueueOperation();
                            } 
                        }
                    }

                    function onReadCompletion(err, resource, responseOptions)
                    {
                        if(err) throw new Error(JSON.stringify(err));
                        if(isOperationLastStep)
                        {
                            isOperationLastStep = false;
                            isReadAttempted = false;
                            i++;
                            
                            links.push(resource);
                        }
                        else
                        {
                            isReadAttempted = true;
                            docResource = resource;
                        }
                        
                        moveNext();
                    }
                    
                    function onInsertCompletion(err, resource, responseOptions)
                    {
                        if(err)
                        {
                            if(!isOperationLastStep && err.number == ErrorCodes.Conflict)
                            {
                                isInsertAttempted = true;
                            }
                            else
                            {
                                throw new Error(JSON.stringify(err));
                            }
                        }
                        else
                        {
                            isOperationLastStep = false;
                            isInsertAttempted = false;
                            i++;
                            
                            links.push(resource);
                        }
                        
                        moveNext();
                    }
                    
                    function onDocumentActionCompletion(err, resource, responseOptions)
                    {
                        if(err) throw new Error(JSON.stringify(err));
                        if(isOperationLastStep)
                        {
                            isInsertAttempted = false;
                            isReadAttempted = false;
                            isOperationLastStep = false;
                            i++;
                            
                            links.push(resource);
                        }
                        
                        moveNext();
                    }

                    function moveNext() {
                        if (i < documents.length) {
                            ProcessBatch();
                        } else {
                            setResponseBody();
                        }
                    }
                    
                    //unable to break batch into multiple continuations since transactional behavior is important.
                    function failedToEnqueueOperation(i) {
                        throw new Error('Failed to enqueue operation ' + i);
                    }

                    function setResponseBody() {
                        getContext().getResponse().setBody(JSON.stringify(links));
                    }
                }
                ";
            return scriptTemplate;
        }
    }
}
