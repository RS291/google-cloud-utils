from io import BufferedReader
from typing import Optional, Union

import google.auth
from google.cloud import storage  # google-cloud-storage


class CloudStorageClient(object):
    def __init__(
        self,
        bucket: Optional[Union[storage.Bucket, str]] = None,
        credentials: Optional[google.auth.credentials.Credentials] = None,
        project: Optional[str] = None,
    ):
        """Can define a default bucket, credentials, and project"""
        self.client = storage.Client(credentials=credentials, project=project)
        if bucket is not None:
            if type(bucket) == storage.Bucket:
                self.bucket = bucket
            elif type(bucket) == str:
                self.bucket = self.client.bucket(bucket)
            else:
                raise TypeError("Bucket not recognized")

    def _bucket_handler(self, bucket: Union[storage.Bucket, str] = None):
        if bucket is None:
            return self.bucket
        if type(bucket) == str:
            return self.client.bucket(bucket)
        elif type(bucket) == storage.Bucket:
            return bucket
        else:
            raise TypeError("Bucket not recognized")

    def _blob_handler(
        self,
        blob: Union[storage.Blob, str],
        bucket: Union[storage.Bucket, str],
    ):
        if type(blob) == str:
            instance = bucket.get_blob(blob)
            if instance:
                return instance
            else:
                return storage.Blob(blob, bucket)
        elif isinstance(blob, storage.Blob):
            return blob
        else:
            raise TypeError("Neither filename nor blob as input")

    def list_blobs(
        self,
        prefix: str or None = None,
        bucket: Optional[Union[storage.Bucket, str]] = None,
    ):
        if prefix is None:
            prefix = ""
        bucket = self._bucket_handler(bucket)
        return list(self.client.list_blobs(bucket, prefix=prefix))

    def get_blob(
        self,
        blob: Union[storage.Blob, str],
        bucket: Optional[Union[storage.Bucket, str]] = None,
        fmt: type = str,
    ):
        bucket = self._bucket_handler(bucket)
        blob = self._blob_handler(blob, bucket)
        if fmt == str:
            blob_string = blob.download_as_text()
        elif fmt == bytes:
            blob_string = blob.download_as_bytes()
        return blob_string

    def create_blob(
        self,
        blob: Union[storage.Blob, str],
        data: str or BufferedReader,
        content_type: str = "text/plain",
        from_file: bool = False,
        bucket: Optional[Union[storage.Bucket, str]] = None,
    ):
        """
        WILL OVERWRITE EXISTING BLOB!
        Creates new/overwrites existing blob and returns blob info.

        Args:
            blob: name of blob (incl virtual path if needed)
            data: data to be written, or path-to-file if directly writing from file
            content_type: Examples are 'text/plain', 'text/csv", 'application/json', etc.
            from_file: Whether or not to directly write from file.
            bucket: name of bucket (if empty, client defined bucket is used)

        """
        bucket = self._bucket_handler(bucket)
        blob = self._blob_handler(blob, bucket)
        if from_file:
            blob.upload_from_filename(data, content_type)
        else:
            blob.upload_from_string(data, content_type)
        return blob

    def delete_blob(
        self,
        blob: Union[storage.Blob, str],
        bucket: Optional[Union[storage.Bucket, str]] = None,
    ):
        bucket = self._bucket_handler(bucket)
        blob = self._blob_handler(blob, bucket)
        blob.delete()
        return True


if __name__ == "__mvp__":
    bucket = "mw-cache-experiment"
    project = "mw-etl-sandbox"
    cs = CloudStorageClient(bucket=bucket, project=project)
    # list all blobs
    blobs = cs.list_blobs()
    # get one blob
    cs.get_blob(blobs[0])
    # write one blob
    blob = cs.create_blob("never/gonna/give/you/up", "never gonna let you down")
    # get that blob
    cs.get_blob(blob)
    # delete that blob
    cs.delete_blob(blob)
