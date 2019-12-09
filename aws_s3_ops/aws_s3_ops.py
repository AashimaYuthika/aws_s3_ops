"""
This module is responsible for s3 I/O Operations.
"""

__author__ = "Aashima Yuthika"
__version = "0.0.0"

import gzip
import os
import pickle
from io import BytesIO

import boto3
import pandas as pd

from aws_s3_ops.exceptions import *


class S3Operations(object):

    def __init__(self, profile_name=None):
        self._session = boto3.Session(profile_name=profile_name)
        self._s3 = self._session.resource('s3')
        self._client = self._session.client('s3')

    def save_pickle(self, bucket, key, obj):
        """
        Takes in an object, pickles it and saves it to the desired s3 location.

        :param bucket: The bucket to which the pickle is to be saved
        :param key: The path inside the bucket, with the filename and extension
        :param obj: The object to be pickled

        :return: Returns true if object was successfully saved
        """
        pickled_obj = pickle.dumps(obj)

        self._s3.Object(bucket, key).put(Body=pickled_obj)

        return self.key_exists(bucket, key)

    def load_pickle(self, bucket, key):
        """
        Takes in the location of a pickle file, and returns the unpickled object for the same.

        :param bucket: The bucket from which the pickle is to be read
        :param key: The path inside the bucket, including the filename and extension for the pickle file

        :return: Returns the unpickled object
        """

        with BytesIO() as obj_buffer:
            self._s3.Bucket(bucket).download_fileobj(key, obj_buffer)
            obj_buffer.seek(0)
            obj = pickle.load(obj_buffer)

        return obj

    @staticmethod
    def _gzip_str(string_):
        """
        Creates a gzip string for saving a dataframe to s3.

        :param string_: The dataframe/string that is to be compressed and saved

        :return: Returns the compressed bytes for boto3 consumption
        """
        out = BytesIO()

        with gzip.GzipFile(fileobj=out, mode='w') as fo:
            fo.write(string_.encode())

        bytes_obj = out.getvalue()
        return bytes_obj

    def save_csv(self, bucket, key, df, compression=None, **kwargs):
        """
        Saves the results (pandas dataframe) to the given s3 bucket and location.

        :param bucket: The name of the bucket in which the dataframe is to be saved
        :param key: The actual path within the bucket, including the filename and extension
            in which the dataframe is to be saved
        :param df: The dataframe that is to be saved
        :param compression: The compression of the file, if it is to be compressed

        :return: Returns a boolean indicating whether the file was successfully uploaded or not
        """
        data_obj = df.to_csv(**kwargs)

        if compression == "gzip":
            data_obj = self._gzip_str(data_obj)

        elif compression is not None and compression != "gzip":
            raise UnsupportedCompressionException(compression)

        self._s3.Object(bucket, key).put(Body=data_obj, ContentType="text/csv")

        return self.key_exists(bucket, key)

    def save_json(self, bucket, key, df, compression=None, **kwargs):
        """
        Saves the results (pandas dataframe) to the given s3 bucket and location.

        :param bucket: The name of the bucket in which the dataframe is to be saved
        :param key: The actual path within the bucket, including the filename and extension
            in which the dataframe is to be saved
        :param df: The dataframe that is to be saved
        :param compression: The compression of the file, if it is to be compressed

        :return: Returns a boolean indicating whether the file was successfully uploaded or not
        """
        data_obj = df.to_json(**kwargs)

        if compression == "gzip":
            data_obj = self._gzip_str(data_obj)

        elif compression is not None and compression != "gzip":
            raise UnsupportedCompressionException(compression)

        self._s3.Object(bucket, key).put(Body=data_obj, ContentType="application/json")

        return self.key_exists(bucket, key)

    def read_csv(self, bucket, key, with_prefix=False, file_extension=None, **kwargs):
        """
        Reads one csv file or multiple csv files, and returns it in a pandas dataframe.

        :param bucket: The name of the bucket from which the file(s) is to be read
        :param key: The exact key for the csv file, with name and extension,  which is to be read
        :param with_prefix: Boolean indicating whether one file is to be read, or multiple.
            True indicates that multiple files need to be read.
        :param file_extension: The file extension string if multiple files are to be read.
            This is used when with_prefix variable is True.

        :return: Returns a pandas dataframe of the csv file(s) that was read
        """
        if with_prefix:
            files = self.get_prefix_object(bucket=bucket,
                                           key=key,
                                           file_extension=file_extension)
            df = pd.concat([pd.read_csv(self.get_file_buffer(bucket=bucket, key=key), **kwargs) for key in files])

        else:
            df = pd.read_csv(self.get_file_buffer(bucket=bucket, key=key), **kwargs)

        return df

    def read_json(self, bucket, key, with_prefix=False, file_extension=None, **kwargs):
        """
        Reads one json file or multiple json files, and returns it in a pandas dataframe.

        :param bucket: The name of the bucket from which the file(s) is to be read
        :param key: The exact key for the json file, with name and extension, which is to be read
        :param with_prefix: Boolean indicating whether one file is to be read, or multiple.
            True indicates that multiple files need to be read.
        :param file_extension: This returns a pandas dataframe of the csv file(s) that was read

        :return: Returns a pandas dataframe of the json file(s) that was read
        """
        if with_prefix:
            files = self.get_prefix_object(bucket=bucket,
                                           key=key,
                                           file_extension=file_extension)

            df = pd.concat([pd.read_json(self.get_file_buffer(bucket=bucket, key=key), **kwargs) for key in files])

        else:
            df = pd.read_json(self.get_file_buffer(bucket=bucket, key=key), **kwargs)

        return df

    def download_file(self, bucket, key, local_path):
        """
        Downloads any given file from the specified location to a local path.

        :param bucket: The bucket from which the file is to be read
        :param key: The exact path within the bucket for the file, including the filename and extension
        :param local_path: The local path to which the specified file is to be downloaded

        :return: Returns a boolean, indicating whether the file has been downloaded to the appropriate
            local path or not
        """

        if self.key_exists(bucket, key):
            self._s3.Bucket(bucket).download_file(key, local_path)

        else:
            raise S3FileNotFoundException("File Not Found - " + key)

        return os.path.isfile(local_path)

    def upload_file(self, bucket, key, local_path):
        """
        Uploads a given local file to the specified s3 location.

        :param bucket: The bucket to which the file is to be uploaded
        :param key: The exact path within the bucket for the file, including the filename and extension
        :param local_path: The local path of the file which the is to be uploaded

        :return: Returns a boolean indicating whether the file has been uploaded to the appropriate s3 path or not
        """
        if os.path.isfile(local_path):
            self._s3.Bucket(bucket).upload_file(local_path, key)

        else:
            raise LocalFileNotFoundException("File Not Found - " + local_path)

        return self.key_exists(bucket, key)

    def key_exists(self, bucket, key):
        """
        Checks if a given file exists on s3 or not.

        :param bucket: The bucket in which the file's existence is to be checked
        :param key: The exact path within the bucket, including the filename, which is to be checked

        :return: Returns a boolean indicating whether the said file exists on s3 or not
        """

        return len(list(self._s3.Bucket(bucket).objects.filter(Prefix=key))) > 0

    def delete_data(self, bucket, key):
        """
        Deletes any given key (folder or file) on s3.

        :param bucket: The bucket from which a folder/file is to be deleted
        :param key: The exact path within the bucket of the folder/file that is to be delete.
            In case of file, mention the exact filename with the appropriate extension.

        :return: Returns a boolean indicating whether the folder/file was successfully deleted from s3 or not
        """

        self._s3.Bucket(bucket).objects.filter(Prefix=key).delete()

        return not self.key_exists(bucket, key)

    def get_prefix_object(self, bucket, key, file_extension=None):
        """
        Gets a list of all objects for a given prefix.

        :param bucket: The bucket from which the list of objects is to be extracted
        :param key: The exact prefix (folder path) whose object list is to be extracted
        :param file_extension: The file_extension (if applicable) whose paths should be returned

        :return: Returns a list of keys for all objects within the specified path
        """

        prefix_objs = self._s3.Bucket(bucket).objects.filter(Prefix=key)

        if file_extension:
            obj_list = [x.key for x in prefix_objs if x.key.endswith(file_extension)]
        else:
            obj_list = [x.key for x in prefix_objs]

        return obj_list

    def get_file_buffer(self, bucket, key):
        """
        Gets the file buffer of a given file on s3, which can then be used to further read the file using pandas
        or other simple python functions.

        :param bucket: The bucket from which the file's buffer is to be extracted
        :param key: The exact path, with the filename and extension,
            within the bucket for which the file buffer is to be extracted

        :return: Returns the file buffer for the specified s3 file,
            which can then be used to read the file in python
        """

        obj = self._client.get_object(Bucket=bucket,
                                      Key=key)

        package = BytesIO(obj['Body'].read())

        return package

    def __del__(self):
        del self._session
        del self._s3
        del self._client
