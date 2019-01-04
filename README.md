# Azure Storage Blob Service, upload with asyncio
This repository contains an example to upload files of any size to Azure Storage Blob Service, using its REST api with `asyncio` and `aiohttp`.
This code was shared, in relation to [this thread in GitHub](https://github.com/Azure/azure-storage-python/issues/534#issuecomment-451260323).

## Example of concurrent files upload

```python

async with SSLClientSession() as http_client:
    client = BlobsClient(http_client, BlockBlobService(ACCOUNT_NAME, ACCOUNT_KEY))

    # NB: this code does not create containers automatically!

    destination_container_name = 'test'

    files = [
        r'one.ext',
        r'two.ext',
        r'three.ext',
        r'four.ext'
    ]

    await asyncio.gather(*[client.upload_file(file_path, destination_container_name) for file_path in files])
```

## Note
When uploading big files to blob service, it is necessary to do several web requests for each file: one for every chunk and a
last one to commit the file. My code intentionally doesn't start parallel web requests to upload chunks of the same file,
because it was designed having in mind a scenario where many files are read from file system and uploaded concurrently (concurrent upload of different files,
not concurrent uploads of _chunks_ of every single file!).

Moreover, concurrent chunk uploads, or concurrent file uploads without limits, could cause to handle too many bytes in memory at the same time - potentially defeating the purpose of chunking big files
on the client side. For this reason, it is recommended to use a [semaphore](https://docs.python.org/3/library/asyncio-sync.html#semaphore), to limit the concurrency of upload operations. There is no perfect 'one-size-fits-all' solution.

If you need a scenario where you handle few files at a given time, then you should change the code to support parallel uploads
of chunks of every single file.
