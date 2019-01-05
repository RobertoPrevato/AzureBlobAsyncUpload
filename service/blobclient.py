import os
import base64
import logging
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
from aiohttp.client import ClientSession
from azure.storage.blob import BlockBlobService, BlobPermissions
from utils.files import get_file_name_from_path, get_best_mime_type, get_file_extension_from_name, read_file_chunks


logger = logging.getLogger('blobs-client')


class MissingOrEmpty(Exception):

    def __init__(self, parameter_name):
        super().__init__(f'Missing or empty {parameter_name}')


class BlobClientException(Exception):
    pass


class UploadFailed(BlobClientException):

    def __init__(self, message: str):
        super().__init__('Upload failed: ' + message)


class DownloadFailed(BlobClientException):

    def __init__(self, message: str):
        super().__init__('Download failed: ' + message)


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

    @staticmethod
    def _validate_file_path(file_path: str):
        if not file_path:
            raise MissingOrEmpty('file path')

        path = Path(file_path)

        if not path.exists():
            raise ValueError(f'file_path {file_path} does not exist')

        if path.is_dir():
            # implement your traversing logic if you need this
            raise ValueError('uploading a whole directory is not supported')

        if path.is_symlink():
            raise ValueError(f'given file path {file_path} refers to a sym link')

    async def download_file(self,
                            container_name: str,
                            blob_name: str,
                            destination_file_path: str = None,
                            chunk_size=4 * 1024 * 1024,
                            force: bool = False):
        if not destination_file_path:
            destination_file_path = blob_name

        if not force and os.path.exists(destination_file_path):
            raise ValueError(f'A file exists at {destination_file_path}')

        with open(destination_file_path, mode='wb') as output_file:
            async for chunk in self.read_blob(container_name, blob_name, chunk_size):
                output_file.write(chunk)

        logging.debug(f'finished downloading blob {blob_name}')

    async def read_blob(self,
                        container_name: str,
                        blob_name: str,
                        chunk_size=4 * 1024 * 1024):
        token = self.get_token(container_name, blob_name)
        file_url = self.base_url + container_name + '/' + blob_name + '?' + token
        current_index = 0

        while True:
            response = await self.http_client.get(file_url, headers={
                'x-ms-range': f'bytes={current_index}-{chunk_size + current_index}'
            })

            if response.status == 416 and current_index > 0:
                # if we get here, it means the blob is exactly long == chunk_size
                return

            if response.status == 206:
                body = await response.content.read()

                logging.debug(f'downloaded chunk of blob {blob_name}')

                yield body

                body_length = len(body)

                if body_length < chunk_size:
                    break

                current_index += body_length
            else:
                text = await response.text()
                raise DownloadFailed(text)

    async def upload_file(self,
                          file_path: str,
                          container_name: str,
                          assigned_file_name: str = None,
                          chunk_size: int = 4 * 1024 * 1024):
        self._validate_file_path(file_path)

        if not container_name:
            raise MissingOrEmpty('container name')

        file_size = os.path.getsize(file_path)
        file_type = get_best_mime_type(file_path)
        original_file_name = get_file_name_from_path(file_path)

        if not assigned_file_name:
            assigned_file_name = original_file_name

        base_url = self.base_url + container_name + '/'

        sas = self.get_token(container_name, assigned_file_name, create=True, write=True)
        submit_url = base_url + assigned_file_name + '?' + sas

        block_ids = await self._upload_chunks(submit_url,
                                              read_file_chunks(file_path, chunk_size),
                                              original_file_name,
                                              file_size)

        await self._complete_upload(submit_url, original_file_name, file_type, block_ids)

    async def _upload_chunks(self,
                             submit_url,
                             chunks_provider,
                             file_name,
                             file_size):
        block_id_prefix = 'block-'
        block_ids = []
        bytes_uploaded = 0
        i = 0

        async for chunk in chunks_provider:
            if not chunk:
                break

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
                raise UploadFailed(text)

            bytes_uploaded += chunk_len

            logger.debug(f'uploaded {bytes_uploaded} / {file_size} for file {file_name}')

        logger.debug(f'finished uploading chunks of {file_name}')
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
            raise UploadFailed('upload commit failed' + text)

        logger.debug(f'completed the upload of {file_name}')
