# Azure Storage Blob Service, upload with asyncio
This repository contains an example to upload files of any size to Azure Storage Blob Service, using its REST api with `asyncio` and `aiohttp`.
This code was shared, in relation to [this thread in GitHub](https://github.com/Azure/azure-storage-python/issues/534#issuecomment-451260323).

## Note
When uploading big files to blob service, it is necessary to do several web requests for each file: one for every chunk and a
last one to commit the file. My example intentionally doesn't start parallel web requests to upload chunks of the same file,
because it was designed having in mind a scenario where many files are read from file system and uploaded concurrently (concurrent upload of different files,
not concurrent uploads of _chunks_ of every single file!).

If you need a scenario where you handle few files at a given time, then you should change the code to support parallel uploads
of chunks of every single file.