

## Assign a "managed service identity" (MSI) to the Function App

![MSI Setup][pictureMSI]

## Grant "Reader" privilege on the subscription (or a resource group)

![MSI becomes Reader on Subscription][pictureSubscriptionIam]

## Grant "Storage Blob Data Contributor" permissions on your storage accounts

![MSI becomes Blob Contributor on Storage][pictureStorageIam]

## Observe what the time-triggered function is doing

![Logging][pictureLogs]






[pictureMSI]: docs/managed-service-identity.png
[pictureSubscriptionIam]: docs/subscription-iam.png
[pictureStorageIam]: docs/storage-iam.png
[pictureLogs]: docs/logs.png






