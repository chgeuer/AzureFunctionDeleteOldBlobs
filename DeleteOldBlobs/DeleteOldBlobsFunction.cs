namespace DeleteOldBlobsFunction
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure;
    using Microsoft.Azure.Management.Resources;
    using Microsoft.Azure.Management.Resources.Models;
    using Microsoft.Azure.Management.Storage;
    using Microsoft.Azure.Management.Storage.Models;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Azure.Subscriptions;
    using Microsoft.Azure.Subscriptions.Models;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

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
            await subscriptionClient.IterateSubscriptions(ct, 
                subscription => ProcessSubscriptionAsync(
                    subscription, msiArmTokenString, log, ct));
        }

        private static async Task ProcessSubscriptionAsync(Subscription subscription, string msiArmTokenString, ILogger log, CancellationToken ct)
        {
            log.LogInformation($"Subscription: {subscription.SubscriptionId} {subscription.DisplayName}");
            if (!IsBackupSubscription(subscription)) { return; }

            var subscriptionCredential = new TokenCloudCredentials(subscriptionId: subscription.SubscriptionId, token: msiArmTokenString);
            log.LogInformation($"subscriptionCredential {subscriptionCredential.ToString()}");

            var resourceManagementClient = new ResourceManagementClient(credentials: subscriptionCredential);
            await resourceManagementClient.IterateResourceGroups(ct, 
                resourceGroupExtended => ProcessResourceGroup(
                    subscriptionCredential: subscriptionCredential, resourceGroupExtended: resourceGroupExtended, log: log, ct: ct));
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

                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                await blobClient.IterateContainers(ct, container => ProcessContainerAsync(account, blobClient, container, log, ct));
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
                await containerReference.IterateBlobs(ct, blob => DeleteBlobAsync(blobClient, blob, log, ct));
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

    public static class MyIteratorExtensions
    {
        public static async Task IterateSubscriptions(this SubscriptionClient subscriptionClient, CancellationToken ct, Func<Subscription, Task> action)
        {
            SubscriptionListResult subscriptionListResult = await subscriptionClient.Subscriptions.ListAsync(ct);
            string subscriptionContinuationToken = subscriptionListResult.NextLink;
            foreach (var subscription in subscriptionListResult.Subscriptions)
            {
                await action(subscription);
            }
            while (!string.IsNullOrEmpty(subscriptionContinuationToken))
            {
                subscriptionListResult = await subscriptionClient.Subscriptions.ListNextAsync(nextLink: subscriptionContinuationToken, cancellationToken: ct);
                subscriptionContinuationToken = subscriptionListResult.NextLink;
                foreach (var subscription in subscriptionListResult.Subscriptions)
                {
                    await action(subscription);
                }
            }
        }

        public static async Task IterateResourceGroups(this ResourceManagementClient resourceManagementClient, CancellationToken ct, Func<ResourceGroupExtended, Task> action)
        {
            ResourceGroupListResult resourceGroupListResult = await resourceManagementClient.ResourceGroups.ListAsync(new ResourceGroupListParameters { });
            string nextLink = resourceGroupListResult.NextLink;
            foreach (var resourceGroupExtended in resourceGroupListResult.ResourceGroups)
            {
                await action(resourceGroupExtended);
            }
            while (!string.IsNullOrEmpty(nextLink))
            {
                resourceGroupListResult = await resourceManagementClient.ResourceGroups.ListNextAsync(nextLink, ct);
                nextLink = resourceGroupListResult.NextLink;
                foreach (var resourceGroupExtended in resourceGroupListResult.ResourceGroups)
                {
                    await action(resourceGroupExtended);
                }
            }
        }

        public static async Task IterateContainers(this CloudBlobClient blobClient, CancellationToken ct, Func<CloudBlobContainer, Task> action)
        {
            BlobContinuationToken containerEnumerationToken = null;
            do
            {
                var response = await blobClient.ListContainersSegmentedAsync(continuationToken: containerEnumerationToken, cancellationToken: ct);
                foreach (var container in response.Results)
                {
                    await action(container);
                }
                containerEnumerationToken = response.ContinuationToken;
            }
            while (containerEnumerationToken != null);
        }

        public static async Task IterateBlobs(this CloudBlobContainer containerReference, CancellationToken ct, Func<IListBlobItem, Task> action)
        {
            BlobContinuationToken blobEnumerationToken = null;
            do
            {
                var blobResponse = await containerReference.ListBlobsSegmentedAsync(currentToken: blobEnumerationToken, cancellationToken: ct);
                foreach (var blob in blobResponse.Results)
                {
                    await action(blob);
                }
                blobEnumerationToken = blobResponse.ContinuationToken;
            }
            while (blobEnumerationToken != null);
        }
    }
}
