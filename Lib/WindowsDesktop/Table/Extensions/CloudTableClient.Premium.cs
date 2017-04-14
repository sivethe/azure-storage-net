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
    using System.Collections.ObjectModel;
    using System.Configuration;
    using Microsoft.Azure.Documents.Client;

    public partial class CloudTableClient
    {
        private DocumentClient documentClient;

        internal DocumentClient DocumentClient
        {
            get
            {
                if (this.documentClient == null)
                {
                    ConnectionPolicy connectionPolicy = null;
                    connectionPolicy = this.ShouldUseGatewayModeMode() 
                        ? new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway } 
                        : new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Azure.Documents.Client.Protocol.Tcp };

                    Collection<string> preferredLocations = this.GetPreferredLocations();
                    if (preferredLocations != null)
                    {
                        foreach (string preferredLocation in preferredLocations)
                        {
                            connectionPolicy.PreferredLocations.Add(preferredLocation);
                        }
                    }

                    Uri accountUri = this.StorageUri.PrimaryUri;
                    string key = this.Credentials.ExportBase64EncodedKey();
                    this.documentClient = new DocumentClient(accountUri, key, connectionPolicy);
                }

                return this.documentClient;
            }
        }

        public bool IsStellarEndpoint()
        {
            // TODO:  Fix this 
            string absoluteUri = this.StorageUri.PrimaryUri.OriginalString;
            return absoluteUri.Contains("localhost") || absoluteUri.Contains("documents.azure.com");
        }

        private bool ShouldUseGatewayModeMode()
        {
            string useGatewayModeValue = ConfigurationManager.AppSettings["DocumentDbUseGatewayMode"];
            bool useGatewayMode = false;
            if (string.IsNullOrEmpty(useGatewayModeValue) || !bool.TryParse(useGatewayModeValue, out useGatewayMode))
            {
                return false;
            }

            return useGatewayMode;
        }

        private Collection<string> GetPreferredLocations()
        {
            string preferredLocationsValue = ConfigurationManager.AppSettings["DocumentDbPreferredLocations"];
            if (string.IsNullOrEmpty(preferredLocationsValue))
            {
                return null;
            }

            return new Collection<string>(preferredLocationsValue.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
        }
    }
}
