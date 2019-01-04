import base64
import logging
from typing import List
from datetime import datetime, timedelta
from aiohttp.client import ClientSession
from azure.storage.blob import BlockBlobService, BlobPermissions


logger = logging.getLogger('blobs-client')


class UploadFailedException(Exception):

    def __init__(self, message: str):
        super().__init__('Upload failed: ' + message)


class BlobsClient:

    def __init__(self,
                 http_client: ClientSession,
                 blob_service: BlockBlobService):
        self.http_client = http_client
        self.blob_service = blob_service
        self.base_url = f'https://{blob_service.account_name}.blob.core.windows.net/'

    def get_token(self, container_name, file_name, read=True, create=False, write=False):
        # noinspection PyTypeChecker
        return self.blob_service.generate_blob_shared_access_signature(
            container_name,
            file_name,
            BlobPermissions(read=read,
                            create=create,
                            write=write),
            datetime.utcnow() + timedelta(hours=20)
        )

    async def upload_file(self,
                          container_name: str,
                          file_name: str,
                          file_size: int,
                          file_type: str,
                          assigned_file_name: str,
                          chunk_size: int = 4 * 1024 * 1024):
        if not file_type:
            file_type = 'application/octet-stream'

        base_url = self.base_url + container_name + '/'
        assigned_file_name = assigned_name
        sas = self.get_token(container_name, assigned_name)
        submit_url = base_url + assigned_file_name + '?' + sas

        block_ids = await self._upload_chunks(submit_url, None)

        await self._complete_upload(submit_url, original_file_name, 'file_type', block_ids)

    async def _upload_chunks(self,
                             submit_url,
                             chunks_provider):
        block_id_prefix = 'block-'
        block_ids = []
        bytes_uploaded = 0
        i = 0

        async for chunk in chunks_provider:
            i += 1
            chunk_len = len(chunk)
            block_id = block_id_prefix + str(i).rjust(6, '0')
            block_id = base64.b64encode(block_id.encode('utf8')).decode('utf8')
            block_ids.append(block_id)

            response = await self.http_client.put(submit_url + '&comp=block&blockid=' + block_id,
                                                  data=chunk,
                                                  headers={
                                                      'x-ms-blob-type': 'BlockBlob',
                                                      'Content-Length': str(chunk_len)
                                                  })
            if response.status != 201:
                text = await response.text()
                raise UploadFailedException(text)

            bytes_uploaded += chunk_len

            logger.debug(f'[*] uploaded {bytes_uploaded} bytes')

        return block_ids

    @staticmethod
    def _get_block_list_payload(block_ids: List[str]):
        parts = ['<?xml version="1.0" encoding="utf-8"?><BlockList>']
        for block_id in block_ids:
            parts.append(f'<Latest>{block_id}</Latest>')
        parts.append('</BlockList>')
        return ''.join(parts)

    async def _complete_upload(self,
                               submit_url: str,
                               file_name: str,
                               file_type: str,
                               block_ids: List[str]):
        payload = self._get_block_list_payload(block_ids)

        headers = {
            'x-ms-blob-content-type': file_type,
            'x-ms-meta-original_name': base64.b64encode(file_name.encode('utf8')).decode('utf8'),
            'Content-Length': str(len(payload))
        }

        response = await self.http_client.put(submit_url + '&comp=blocklist',
                                              data=payload.encode('utf8'),
                                              headers=headers)

        if response.status != 201:
            text = await response.text()
            raise UploadFailedException('upload commit failed' + text)
