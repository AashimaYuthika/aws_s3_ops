"""
This module is responsible for s3 I/O Operations.
"""

__author__ = "Aashima Yuthika"
__version = "0.0.0"

import pickle
from io import BytesIO

import boto3


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

    def key_exists(self, bucket, key):
        """
        Checks if a given file exists on s3 or not.

        :param bucket: The bucket in which the file's existence is to be checked
        :param key: The exact path within the bucket, including the filename, which is to be checked

        :return: Returns a boolean indicating whether the said file exists on s3 or not
        """

        return len(list(self._s3.Bucket(bucket).objects.filter(Prefix=key))) > 0

    def __del__(self):
        del self._session
        del self._s3
        del self._client
