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
    using Microsoft.Azure.Management.Resources;

    public static class DeleteOldBlobs
    {
        [FunctionName("DeleteOldBlobsFunction46")]
        public async static Task Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            var cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;

            try
            {
                await ProcessAllSubscriptions(log, ct);
            }
            catch (Exception ex)
            {
                log.LogError($"Exception: {ex.GetType().FullName} \"{ex.Message}\" - {ex.StackTrace}");
            }
        }

        private static Task<string> GetCredentialAsync(string resource)
        {
            var azureServiceTokenProvider = new AzureServiceTokenProvider();
            return azureServiceTokenProvider.GetAccessTokenAsync(resource: resource);
        }

        private static async Task ProcessAllSubscriptions(ILogger log, CancellationToken ct)
        {
            string msiArmTokenString = await GetCredentialAsync(resource: "https://management.core.windows.net/");

            var subscriptionClient = new SubscriptionClient(credentials: new TokenCloudCredentials(msiArmTokenString));
            string subscriptionContinuationToken = string.Empty;
            do
            {
                var subscriptionListResult = await subscriptionClient.Subscriptions.ListNextAsync(nextLink: subscriptionContinuationToken, cancellationToken: ct);
                foreach (var subscription in subscriptionListResult.Subscriptions)
                {
                    await ProcessSubscriptionAsync(subscription, msiArmTokenString, log, ct);
                }
                subscriptionContinuationToken = subscriptionListResult.NextLink;
            }
            while (!string.IsNullOrEmpty(subscriptionContinuationToken));
        }

        private static async Task ProcessSubscriptionAsync(Subscription subscription, string msiArmTokenString, ILogger log, CancellationToken ct)
        {
            log.LogInformation($"Subscription: {subscription.SubscriptionId} {subscription.DisplayName}");
            if (!IsBackupSubscription(subscription)) { return; }

            var subscriptionCredential = new TokenCloudCredentials(subscriptionId: subscription.SubscriptionId, token: msiArmTokenString);
            log.LogInformation($"subscriptionCredential {subscriptionCredential.ToString()}");

            var resourceManagementClient = new ResourceManagementClient(credentials: subscriptionCredential);
            // var resourceGroupListResult = await resourceManagementClient.ResourceGroups.ListAsync(new ResourceGroupListParameters { });
            string nextLink = null;
            do
            {
                var resourceGroupListResult = await resourceManagementClient.ResourceGroups.ListNextAsync(nextLink, ct);
                foreach (var resourceGroupExtended in resourceGroupListResult.ResourceGroups)
                {
                    await ProcessResourceGroup(subscriptionCredential: subscriptionCredential, resourceGroupExtended: resourceGroupExtended, log: log, ct: ct);
                }
                nextLink = resourceGroupListResult.NextLink;
            } while (!string.IsNullOrEmpty(nextLink));
        }

        private static async Task ProcessResourceGroup(TokenCloudCredentials subscriptionCredential, ResourceGroupExtended resourceGroupExtended, ILogger log, CancellationToken ct)
        {
            log.LogInformation($"Resource Group {resourceGroupExtended.Id}");

            var storageManagementClient = new StorageManagementClient(credentials: subscriptionCredential);
            var storageAccountListResponse = await storageManagementClient.StorageAccounts.ListByResourceGroupAsync(resourceGroupName: resourceGroupExtended.Name, cancellationToken: ct);
            foreach (var account in storageAccountListResponse.StorageAccounts)
            {
                var keys = await storageManagementClient.StorageAccounts.ListKeysAsync(resourceGroupName: resourceGroupExtended.Name, accountName: account.Name);
                await ProcessStorageAccountAsync(account, keys, log, ct);
            }
        }

        private static async Task ProcessStorageAccountAsync(StorageAccount account, StorageAccountListKeysResponse keys, ILogger log, CancellationToken ct)
        {
            try
            {
                log.LogInformation($"Found storage account: {account.Name}");
                if (!IsBackupStorageAccount(account)) { return; }

                //var azureServiceTokenProvider = new AzureServiceTokenProvider();
                //var endpointSuffix = "core.windows.net";
                //var storageTokenString = await azureServiceTokenProvider.GetAccessTokenAsync(resource: $"https://{account.Name}.blob.{endpointSuffix}/");
                //var storageToken = new StorageCredentials(tokenCredential: new TokenCredential(initialToken: storageTokenString));
                //var storageAccount = new CloudStorageAccount(storageCredentials: storageToken, accountName: account.Name, endpointSuffix: endpointSuffix, useHttps: true);

                var storageAccount = new CloudStorageAccount(
                    storageCredentials: new StorageCredentials(
                        accountName: account.Name, 
                        keyValue: keys.StorageAccountKeys.Key1), 
                    useHttps: true);

                var blobClient = storageAccount.CreateCloudBlobClient();

                BlobContinuationToken containerEnumerationToken = null;
                do
                {
                    var response = await blobClient.ListContainersSegmentedAsync(continuationToken: containerEnumerationToken, cancellationToken: ct);
                    foreach (var container in response.Results)
                    {
                        await ProcessContainerAsync(account, blobClient, container, log, ct);
                    }
                    containerEnumerationToken = response.ContinuationToken;
                }
                while (containerEnumerationToken != null);
            }
            catch (Exception accountException)
            {
                log.LogError($"{accountException.GetType().FullName} while processing account {account.Name}: \"{accountException.Message}\" - {accountException.StackTrace}");
            }
        }

        private static async Task ProcessContainerAsync(StorageAccount account, CloudBlobClient blobClient, CloudBlobContainer container, ILogger log, CancellationToken ct)
        {
            log.LogInformation($"Found container: {account.Name}/{container.Name}");
            if (!IsBackupContainerName(container)) { return; }

            try
            {
                CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName: container.Name);
                BlobContinuationToken blobEnumerationToken = null;
                do
                {
                    var blobResponse = await containerReference.ListBlobsSegmentedAsync(currentToken: blobEnumerationToken, cancellationToken: ct);
                    foreach (var blob in blobResponse.Results)
                    {
                        await DeleteBlobAsync(blobClient, blob, log, ct);
                    }
                    blobEnumerationToken = blobResponse.ContinuationToken;
                }
                while (blobEnumerationToken != null);
            }
            catch (Exception containerException)
            {
                log.LogError($"{containerException.GetType().FullName} while processing container {container.StorageUri.PrimaryUri.AbsoluteUri}: \"{containerException.Message}\" - {containerException.StackTrace}");
            }
        }

        private static async Task DeleteBlobAsync(CloudBlobClient blobClient, IListBlobItem blob, ILogger log, CancellationToken ct)
        {
            try
            {
                log.LogInformation($"Found blob {blob.Uri}");
                var reference = await blobClient.GetBlobReferenceFromServerAsync(blobUri: blob.Uri, cancellationToken: ct);
                if (!IsBackupBlob(reference)) { return; }

                if (ShouldBeDeleted(reference, log))
                {
                    log.LogInformation($"Delete blob {blob.Uri}");

                    // await reference.DeleteIfExistsAsync(cancellationToken: ct);
                }
            }
            catch (Exception blobException)
            {
                log.LogError($"{blobException.GetType().FullName} while processing blob {blob.Uri.AbsoluteUri}: \"{blobException.Message}\" - {blobException.StackTrace}");
            }
        }

        private static bool IsBackupSubscription(Subscription subscription) => true;

        private static bool IsBackupResourceGroup(ResourceGroupExtended group) => true;

        private static bool IsBackupStorageAccount(StorageAccount storageAccount) => true; // storageAccount.Name.StartsWith("sadjfksjlahfkj"); // storageAccount.Name.StartsWith("backup");

        private static bool IsBackupContainerName(CloudBlobContainer container) => true; // container.Name.Contains("hec");

        private static bool IsBackupBlob(ICloudBlob blob) => true;

        private static bool ShouldBeDeleted(ICloudBlob blob, ILogger log)
        {
            var age = DateTime.UtcNow - blob.Properties.Created.Value;
            var shouldBeDeleted = TimeSpan.FromDays(30) < age;
            var msg = shouldBeDeleted ? "should be deleted" : "should not be deleted";
            log.LogInformation($"{blob.Uri.AbsoluteUri} is {(int)age.Days} days old and {msg}");
            return shouldBeDeleted;
        }
    }
}
