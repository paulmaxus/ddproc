# Library for working with data donation data

- Downloading
- Filtering, processing
- Converting to tables

The data first have to be downloaded from the respective storage location.
For Azure Blob storage, you can use this library's [download_from_azure()](##downloading-data-from-azure) function.

The library assumes that the data consists of individual *json* files within a *data.zip* folder.

## Downloading data from Azure

First you need to [install the Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).
Then, log in; you only have to do this very rarely.

```
az login
```

Download the data
```
ddp.download_from_azure()
```
