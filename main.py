import asyncio
from service.httpclient import SSLClientSession
from service.blobclient import BlobsClient
from azure.storage.blob import BlockBlobService


ACCOUNT_NAME = ''
ACCOUNT_KEY = ''


class FileInfo:

    def __init__(self):
        pass


def read_file_chunks():
    pass


async def example():

    async with SSLClientSession() as http_client:
        # Recommendation: instead of instantiating classes this way, consider adopting a dependency injection library,
        # like rodi --> https://github.com/RobertoPrevato/rodi

        client = BlobsClient(http_client, BlockBlobService(ACCOUNT_NAME, ACCOUNT_KEY))

        await client.upload_file('container_name', )


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example())


if __name__ == '__main__':
    main()

