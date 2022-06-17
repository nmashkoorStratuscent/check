# Path to Service Account Key
KEY = r''
import os
from google.cloud import storage

class GStore:
    def __init__(self, bucket_id: str=None, root_folder: str=None, key: str=KEY):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY
        self.bucket_id = bucket_id
        if bucket_id and key:
            self.bucket_id = bucket_id
            self.client = storage.Client()
        else:
            raise ValueError('Bucket ID can not be None')

    def update_bucket(self, bucket_id):
        self.bucket_id = bucket_id
    
    def get_bucket_name(self):
        return self.bucket_id

    def list_files(self, path, get_abs = False, recurse=False):
        """Lists paths of all the files in the bucket."""
        if recurse:
            delimiter=None
        else:
            delimiter='/'
        blobs = self.client.list_blobs(self.bucket_id, prefix=path, delimiter=delimiter)
        result = []
        for blob in blobs:
            if blob.name[-1] != '/': 
                if get_abs:
                    result.append(f'gs://{self.bucket_id}/{blob.name}')
                else:
                    result.append(blob.name)
        return result

    def rename_file(self, file_name, new_name):
        """Renames a file."""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(file_name)
        new_blob = bucket.rename_blob(blob, new_name)
        return new_blob.name

    def copy_file(self, source_name, destination_name, delete_org=False):
        """Copies a file from source to destination."""
        bucket = self.client.bucket(self.bucket_id)
        source_file = bucket.blob(source_name)
        copied = bucket.copy_blob(source_file, bucket, destination_name)
        if delete_org:
            bucket.delete_blob(source_name)
        return copied.name

    def delete_file(self, file_name):
        """Deletes a fil from the bucket."""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(file_name)
        blob.delete()

    def is_file(self, file_name):
        """Checks if a file exists"""
        bucket = self.client.bucket(self.bucket_id)
        found = storage.Blob(bucket=bucket, name=file_name).exists(self.client)
        return found

    def is_dir(self, dir_name):
        """Checks if a dir exists"""
        blobs = list(self.client.list_blobs(self.bucket_id, prefix=dir_name))
        return bool(blobs) and len(blobs) != 0

    def rmdir(self, dir_name):
        """Deletes a folder from the bucket."""
        bucket = self.client.bucket(self.bucket_id)
        blobs = bucket.list_blobs(prefix=dir_name)
        for blob in blobs:
            blob.delete()

    def mkdir(self, destination_folder_name):
        """Make a new directory in cloud"""
        bucket = self.client.get_bucket(self.bucket_id)
        if destination_folder_name[-1] != '/':
            destination_folder_name = destination_folder_name + '/'
        blob = bucket.blob(destination_folder_name)
        return blob.upload_from_string('')
        
    def upload_from_memory(self, contents, destination_file_name):
        """Uploads a in memory content to the bucket. The content is expected to be in bytes"""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_string(contents)
        return True

    def upload_file_local(self, source_file_name, dest_file_path):
        """Uplaod a local file to cloud"""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(dest_file_path)
        return blob.upload_from_filename(source_file_name)
    
    def upload__folder(self, source_folder_path, destination_path):
        """Upload a local folder to cloud"""
        if destination_path and destination_path[-1] != '/':
            destination_path = destination_path + '/'
        bucket = self.client.bucket(self.bucket_id)
        file_paths = [os.path.join(folder, f) for folder, dirs, files in os.walk(source_folder_path) for f in files]
        file_paths = list(map(lambda x: x.replace('\\', '/'), file_paths))
        for file_path in file_paths:
            try:
                blob = bucket.blob(destination_path + file_path)
                blob.upload_from_filename(file_path)
            except Exception:
                print(f'Failed saving file:{file_path}')
        return True
    
    def upload_from_memory(self, contents, destination_file_name, content_type=None):
        """Uploads a in memory content to the bucket. The content is expected to be in bytes"""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_string(contents, content_type=content_type)
        return True

    def download_file_local(self, cloud_file_name, dest_file_path):
        """Downloads file into file directory"""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(cloud_file_name)
        return blob.download_to_filename(dest_file_path)

    def download_into_memory(self, file_name):
        """Downloads file into memory"""
        bucket = self.client.bucket(self.bucket_id)
        blob = bucket.blob(file_name)
        contents = blob.download_as_string()
        return contents