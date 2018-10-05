namespace DeleteOldBlobsFunction
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.Subscriptions;
    using Microsoft.Azure.Subscriptions.Models;
    using Microsoft.Azure.Management.Storage;
    using Microsoft.Azure.Management.Storage.Models;
    using Microsoft.Azure.Management.Resources.Models;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.Extensions.Logging;

    public static class DeleteOldBlobs
    {
        private static bool IsBackupSubscription(Subscription subscription) => true;
        private static bool IsBackupResourceGroup(ResourceGroupExtended group) => true;
        private static bool IsBackupStorageAccount(StorageAccount storageAccount) => true; // storageAccount.Name.StartsWith("sadjfksjlahfkj"); // storageAccount.Name.StartsWith("backup");
        private static bool IsBackupContainerName(CloudBlobContainer container) => true; // container.Name.Contains("hec");
        private static bool IsBackupBlob(ICloudBlob blob) => true;

        private static bool ShouldBeDeleted(ICloudBlob blob, ILogger log)
        {
            var age = DateTime.UtcNow - blob.Properties.Created.Value;
            return TimeSpan.FromDays(30) < age;
        }

        [FunctionName("DeleteOldBlobsFunction46")]
        public async static Task Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            var cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;

            //HttpClient client = new HttpClient();
            //client.DefaultRequestHeaders.Add("Secret", Environment.GetEnvironmentVariable("MSI_SECRET"));
            //var t = await client.GetAsync($"{Environment.GetEnvironmentVariable("MSI_ENDPOINT")}/?resource={resource}&api-version={apiversion}");
            try
            {
                log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
                var azureServiceTokenProvider = new AzureServiceTokenProvider();
                var msiArmTokenString = await azureServiceTokenProvider.GetAccessTokenAsync(
                    resource: "https://management.core.windows.net/"
                    // , tenantId: "xxxxxx.onmicrosoft.com");
                    ); 
                var tokenCloudCredential = new TokenCloudCredentials(token: msiArmTokenString);
                var subscriptionClient = new SubscriptionClient(credentials: tokenCloudCredential);

                string subscriptionContinuationToken = null;
                do
                {
                    var subscriptionListResult = await subscriptionClient.Subscriptions.ListAsync(cancellationToken: ct);
                    subscriptionContinuationToken = subscriptionListResult.NextLink;
                    foreach (var subscription in subscriptionListResult.Subscriptions)
                    {
                        log.LogInformation($"Subscription: {subscription.SubscriptionId} {subscription.DisplayName}");
                        if (!IsBackupSubscription(subscription)) { continue; }

                        var subscriptionCredential = new TokenCloudCredentials(
                            subscriptionId: subscription.SubscriptionId, token: msiArmTokenString);
                        log.LogInformation($"subscriptionCredential {subscriptionCredential.ToString()}");

                        //var resourceMgmtClient = new Microsoft.Azure.Management.Resources.ResourceManagementClient(credentials: subscriptionCredential);
                        //var resourceGroupResponse = await resourceMgmtClient.ResourceGroups.ListAsync(
                        //    parameters: new ResourceGroupListParameters(), cancellationToken: ct);
                        //string resourceGroupContinuationToken = null;
                        //do
                        //{
                        //    foreach (var group in resourceGroupResponse.ResourceGroups)
                        //    {
                        //        log.LogInformation($"Resource Group {group.Name}");
                        //        if (!IsBackupResourceGroup(group)) { continue; }
                        //    }
                        //    resourceGroupContinuationToken = resourceGroupResponse.NextLink;
                        //} while (!string.IsNullOrEmpty(resourceGroupContinuationToken));

                        var client = new StorageManagementClient(credentials: subscriptionCredential);
                        log.LogInformation("Created StorageManagementClient");

                        var storageAccountListResponse = await client.StorageAccounts.ListAsync();
                        foreach (var account in storageAccountListResponse.StorageAccounts)
                        {
                            log.LogInformation($"Found storage account: {account.Name}");
                            if (!IsBackupStorageAccount(account)) { continue; }

                            var endpointSuffix = "core.windows.net";
                            var storageTokenString = await azureServiceTokenProvider.GetAccessTokenAsync(
                                resource: $"https://{account.Name}.blob.{endpointSuffix}/");

                            var storageToken = new StorageCredentials(
                                tokenCredential: new TokenCredential(initialToken: storageTokenString));

                            var storageAccount = new CloudStorageAccount(
                                storageCredentials: storageToken, accountName: account.Name, 
                                endpointSuffix: endpointSuffix, useHttps: true);

                            var blobClient = storageAccount.CreateCloudBlobClient();
                            BlobContinuationToken containerEnumerationToken = null;
                            do
                            {
                                var response = await blobClient.ListContainersSegmentedAsync(continuationToken: containerEnumerationToken);
                                containerEnumerationToken = response.ContinuationToken;
                                foreach (var container in response.Results)
                                {
                                    log.LogInformation($"Found {account.Name}/{container.Name}");
                                    if (!IsBackupContainerName(container)) { continue; }

                                    var containerReference = blobClient.GetContainerReference(containerName: container.Name);
                                    BlobContinuationToken blobEnumerationToken = null;
                                    do
                                    {
                                        var blobResponse = await containerReference.ListBlobsSegmentedAsync(
                                            currentToken: blobEnumerationToken, cancellationToken: ct);
                                        blobEnumerationToken = blobResponse.ContinuationToken;
                                        foreach (var blob in blobResponse.Results)
                                        {
                                            log.LogInformation($"Found blob {blob.Uri}");
                                            var reference = await blobClient.GetBlobReferenceFromServerAsync(blobUri: blob.Uri);
                                            if (!IsBackupBlob(reference)) { continue; }

                                            if (ShouldBeDeleted(reference, log))
                                            {
                                                log.LogInformation($"Delete blob {blob.Uri}");
                                            }
                                        }
                                    }
                                    while (blobEnumerationToken != null);
                                }
                            }
                            while (containerEnumerationToken != null);
                        }
                    }
                }
                while (!string.IsNullOrEmpty(subscriptionContinuationToken));
            }
            catch (Exception ex)
            {
                log.LogError($"Exception: {ex.GetType().FullName} \"{ex.Message}\" - {ex.StackTrace}");
            }
        }
    }
}
