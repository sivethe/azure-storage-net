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

using Microsoft.WindowsAzure.Storage.Table.Extensions;

namespace Microsoft.WindowsAzure.Storage.Table.Extension
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.WindowsAzure.Storage.Core;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal static class EntityHelpers
    {
        public static Document GetDocumentFromEntity(ITableEntity entity, OperationContext context, TableRequestOptions options)
        {
            if (entity == null)
            {
                throw new ArgumentException("Entity should not be null.");
            }

            IDictionary<string, EntityProperty> properties = entity.WriteEntity(context);
            if (options != null)
            {
                try
                {
                    options.AssertPolicyIfRequired();

                    if (options.EncryptionPolicy != null)
                    {
                        properties = options.EncryptionPolicy.EncryptEntity(properties, entity.PartitionKey, entity.RowKey, options.EncryptionResolver);
                    }
                }
                catch (Exception ex)
                {
                    throw new StorageException(SR.EncryptionLogicError, ex) { IsRetryable = false };
                }
            }

            using (StringWriter stringWriter = new StringWriter(CultureInfo.InvariantCulture))
            {
                using (ITableEntityWriter entityWriter = new TableEntityWriter(stringWriter))
                {
                    entityWriter.Start();

                    foreach (var property in properties)
                    {
                        entityWriter.WriteName(property.Key);
                        entityWriter.WriteValue(property.Value);
                    }

                    entityWriter.End();
                }

                //$TODO: Write id and partition key directly to string writer
                Document document = JsonConvert.DeserializeObject<Document>(stringWriter.ToString());
                document.Id = entity.RowKey;
                document.SetPropertyValue(StellarConstants.PartitionKeyPropertyName, entity.PartitionKey);

                return document;
            }
        }

        public static object GetValueFromProperty(EntityProperty property)
        {
            switch (property.PropertyType)
            {
                case EdmType.Binary:
                    return property.BinaryValue;

                case EdmType.Boolean:
                    return property.BooleanValue;

                case EdmType.DateTime:
                    return property.DateTime;

                case EdmType.Double:
                    return property.DoubleValue;

                case EdmType.Guid:
                    return property.GuidValue;

                case EdmType.Int32:
                    return property.Int32Value;

                case EdmType.Int64:
                    return property.Int64Value;

                case EdmType.String:
                    return property.StringValue;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static EntityProperty GetPropertyFromToken(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Boolean:
                    return new EntityProperty(token.ToObject<bool>());

                case JTokenType.Bytes:
                    return new EntityProperty(token.ToObject<byte[]>());

                case JTokenType.Date:
                    return new EntityProperty(token.ToObject<DateTime>());

                case JTokenType.Float:
                    return new EntityProperty(token.ToObject<float>());

                case JTokenType.Guid:
                    return new EntityProperty(token.ToObject<Guid>());

                case JTokenType.Integer:
                    return new EntityProperty(token.ToObject<Int32>());

                case JTokenType.String:
                    return new EntityProperty(token.ToObject<string>());

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static TableResult GetTableResultFromResponse(ResourceResponse<DocumentCollection> response, OperationContext context)
        {
            TableResult result = new TableResult();
            result.Etag = response.ResponseHeaders["ETag"];
            result.HttpStatusCode = (int)response.StatusCode;
            return result;
        }

        public static StorageException GetTableResultFromException(Exception exception, TableOperation tableOperation = null)
        {
            return exception.ToStorageException(tableOperation);
        }

        public static ITableEntity GetEntityFromDocument(Document document, OperationContext context)
        {
            DynamicTableEntity entity = new DynamicTableEntity(document.GetPropertyValue<string>(StellarConstants.PartitionKeyPropertyName), document.Id);
            entity.ETag = document.ETag;
            entity.Timestamp = document.Timestamp;
            entity.PartitionKey = document.GetPropertyValue<string>(StellarConstants.PartitionKeyPropertyName);

            Dictionary<string, EntityProperty> entityProperties = new Dictionary<string, EntityProperty>();
            using (ITableEntityReader entityReader = new TableEntityReader(JsonConvert.SerializeObject(document)))
            {
                entityReader.Start();

                while (entityReader.MoveNext())
                {
                    string name = entityReader.ReadCurrentName();
                    EntityProperty property = entityReader.ReadCurrentValue();

                    entityProperties.Add(name, property);
                }

                entityReader.End();
            }

            entity.ReadEntity(entityProperties, context);
            return entity;
        }

        public static IDictionary<string, EntityProperty> DecryptEntityProperties(
            IDictionary<string, EntityProperty> properties,
            TableRequestOptions options,
            ResourceResponse<Document> response)
        {
            // If encryption policy is set on options, try to decrypt the entity.
            EntityProperty propertyDetailsProperty;
            EntityProperty keyProperty;
            byte[] cek = null;
            string pk = response.Resource.GetPropertyValue<string>(StellarConstants.PartitionKeyPropertyName);
            string rk = response.Resource.Id;

            if (properties.TryGetValue(Constants.EncryptionConstants.TableEncryptionPropertyDetails, out propertyDetailsProperty)
                && properties.TryGetValue(Constants.EncryptionConstants.TableEncryptionKeyDetails, out keyProperty))
            {
                // Decrypt the metadata property value to get the names of encrypted properties.
                EncryptionData encryptionData = null;
                bool isJavaV1 = false;

                // Convert the base 64 encoded TableEncryptionPropertyDetails string stored in DocumentDB to binary
                propertyDetailsProperty = EntityProperty.CreateEntityPropertyFromObject(propertyDetailsProperty.StringValue, EdmType.Binary);

                cek = options.EncryptionPolicy.DecryptMetadataAndReturnCEK(pk, rk, keyProperty, propertyDetailsProperty, out encryptionData, out isJavaV1);

                byte[] binaryVal = propertyDetailsProperty.BinaryValue;
                HashSet<string> encryptedPropertyDetailsSet;

                encryptedPropertyDetailsSet = EntityHelpers.ParseEncryptedPropertyDetailsSet(isJavaV1, binaryVal);

                // The encrypted properties are stored in DocumentDB as base 64 encoded strings. Convert them to binary.
                Dictionary<string, EntityProperty> binEncryptedProperties = new Dictionary<string, EntityProperty>();

                foreach (KeyValuePair<string, EntityProperty> kvp in properties)
                {
                    if (encryptedPropertyDetailsSet.Contains(kvp.Key))
                    {
                        EntityProperty binEncryptedProperty = EntityProperty.CreateEntityPropertyFromObject(kvp.Value.StringValue, EdmType.Binary);
                        binEncryptedProperties[kvp.Key] = binEncryptedProperty;
                    }
                }

                foreach (KeyValuePair<string, EntityProperty> kvp in binEncryptedProperties)
                {
                    properties[kvp.Key] = binEncryptedProperties[kvp.Key];
                }

                properties = options.EncryptionPolicy.DecryptEntity(properties, encryptedPropertyDetailsSet, pk, rk, cek, encryptionData, isJavaV1);
            }
            else
            {
                if (options.RequireEncryption.HasValue && options.RequireEncryption.Value)
                {
                    throw new StorageException(SR.EncryptionDataNotPresentError, null) { IsRetryable = false };
                }
            }

            return properties;
        }

        private static HashSet<string> ParseEncryptedPropertyDetailsSet(bool isJavav1, byte[] binaryVal)
        {
            HashSet<string> encryptedPropertyDetailsSet;
            if (isJavav1)
            {
                encryptedPropertyDetailsSet = new HashSet<string>(Encoding.UTF8.GetString(binaryVal, 1, binaryVal.Length - 2).Split(new char[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries));
            }
            else
            {
                encryptedPropertyDetailsSet = JsonConvert.DeserializeObject<HashSet<string>>(Encoding.UTF8.GetString(binaryVal, 0, binaryVal.Length));
            }

            return encryptedPropertyDetailsSet;
        }

    }
}