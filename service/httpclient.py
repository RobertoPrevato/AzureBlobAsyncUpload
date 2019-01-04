"""This module contains a subclass of aiohttp HTTP client session, using automatically an SSL context with a cafile
 configured, from certifi package. This is to avoid annoying SSL exceptions that might arise from OpenSSL not being
 able to detect a cafile."""
import ssl as actual_ssl
import certifi
from aiohttp import ClientSession
from aiohttp.client import _RequestContextManager
from aiohttp.typedefs import StrOrURL
from aiohttp.helpers import sentinel


default_ssl_context = actual_ssl.create_default_context(cafile=certifi.where())


class SSLClientSession(ClientSession):

    def request(self, method: str, url: StrOrURL, **kwargs) -> _RequestContextManager:
        return super().request(method, url, **kwargs)

    async def _request(self, method, url, *, params=None, data=None, json=None, headers=None, skip_auto_headers=None,
                       auth=None, allow_redirects=True, max_redirects=10, compress=None, chunked=None, expect100=False,
                       raise_for_status=None, read_until_eof=True, proxy=None, proxy_auth=None, timeout=sentinel,
                       verify_ssl=None, fingerprint=None, ssl_context=None, ssl=None, proxy_headers=None,
                       trace_request_ctx=None, **kwargs):
        if ssl_context is None:
            ssl_context = default_ssl_context

        return await super()._request(method, url, params=params, data=data, json=json, headers=headers,
                                      skip_auto_headers=skip_auto_headers, auth=auth, allow_redirects=allow_redirects,
                                      max_redirects=max_redirects, compress=compress, chunked=chunked,
                                      expect100=expect100, raise_for_status=raise_for_status,
                                      read_until_eof=read_until_eof, proxy=proxy, proxy_auth=proxy_auth,
                                      timeout=timeout, verify_ssl=verify_ssl, fingerprint=fingerprint,
                                      ssl_context=ssl_context, ssl=ssl, proxy_headers=proxy_headers,
                                      trace_request_ctx=trace_request_ctx)
