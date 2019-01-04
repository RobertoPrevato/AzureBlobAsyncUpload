import asyncio
import logging
from service.httpclient import SSLClientSession
from service.blobclient import BlobsClient
from azure.storage.blob import BlockBlobService


logging.basicConfig(level=logging.DEBUG)  # adjust as desired


ACCOUNT_NAME = '<YOUR ACCOUNT NAME>'
ACCOUNT_KEY = '<YOUR ACCOUNT KEY>'


async def example():

    async with SSLClientSession() as http_client:
        # Recommendation: instead of instantiating classes this way, consider adopting a dependency injection library,
        # like rodi --> https://github.com/RobertoPrevato/rodi
        client = BlobsClient(http_client, BlockBlobService(ACCOUNT_NAME, ACCOUNT_KEY))

        # NB: this code does not create containers automatically!

        destination_container_name = 'test'

        await client.upload_file(r'C:\Users\someuser\Downloads\example.pdf', destination_container_name)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example())


if __name__ == '__main__':
    main()

