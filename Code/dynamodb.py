import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from time import sleep
from functools import lru_cache
from multiprocessing.dummy import Pool
from botocore.exceptions import ClientError
from pyaws.awslambda import read_env_variable
from libtools.js import export_iterobject
from spotlib import SpotPrices, UtcConversion
import loggers
from _version import __version__

logger = loggers.getLogger(__version__)


def standardize_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def utc_datetime(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def datetimify_standard(s):
    """Function to create timezone unaware string"""
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


@lru_cache()
def get_data(partition_key, value, tableName, region=None):
    """
    Summary.

        Retrieves data from DynamoDB Table

    Args:
        :partition_key (str):  Partition Key Field Name of the Table
        :value (str):  Partition Key value for records we want returned
        :tableName (str): Name of dyanamoDB table
        :region (str): AWS region code denoting the location of the table

    Returns:
        dynamodb table records, TYPE: dict

    """
    key = Key(partition_key).eq(value)

    if region is not None:
        key &= Key('resource_region|hostname').begins_with(region + '|')

    data = tableName.query(KeyConditionExpression=key)['Items']

    return {
        x['resource_region|hostname'].split('|')[-1]: x['instance_status']
        for x in data
    }
