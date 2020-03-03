import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from time import sleep
from functools import lru_cache
from multiprocessing.dummy import Pool
from botocore.exceptions import ClientError
from pyaws.awslambda import read_env_variable
from libtools import logd
from spotlib import SpotPrices, UtcConversion


logger = logd.getLogger('1.0')

# globals
REGION = read_env_variable('REGION', default='eu-west-1')
DB = boto3.resource('dynamodb', region_name=REGION)

METADATA_TABLENAME = read_env_variable('METADATA_TABLENAME', default='MPCAWS_EC2_METADATA')
METADATA_TABLE = DB.Table(METADATA_TABLENAME)

DB_CLIENT = boto3.client('dynamodb', region_name=REGION)


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


class AssignRegion():
    """Map AvailabilityZone to corresponding AWS region"""
    def __init__(self):
        self.client = boto3.client('ec2')
        self.regions = [x['RegionName'] for x in self.client.describe_regions()['Regions']]

    def assign_region(self, az):
        return [x for x in self.regions if x in az][0]


class DynamoDBPrices():
    def __init__(self, table_name, start_date, end_date):
        self.ar = AssignRegion()
        self.sp = SpotPrices(start_dt=start_date, end_dt=end_date)
        self.regions = self.ar.regions
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def insert_dynamodb_record(self, regions=[]):
        """
            Inserts data items into DynamoDB table
                - Partition Key:  Timestamp
                - Sort Key: Spot Price
        Args:
            region_list (list): AWS region code list from which to gen price data

        Returns:
            dynamodb table object
        """
        prices = self.sp.generate_pricedata(regions=regions or self.regions)
        uc = UtcConversion(prices)      # converts datatime objects to str date times
        price_dicts = prices['SpotPriceHistory']

        for item in price_dicts:
            try:
                self.table.put_item(
                    Item={
                            'RegionName':  self.ar.assign_region(item['AvailabilityZone']),
                            'AvailabilityZone': item['AvailabilityZone'],
                            'InstanceType': item['InstanceType'],
                            'ProductDescription': item['ProductDescription'],
                            'SpotPrice': item['SpotPrice'],
                            'Timestamp': item['Timestamp']
                    }
                )
                logger.info(
                    'Successful put item for AZ {} at time {}'.format(item['AvailabilityZone'], item['Timestamp'])
                )
            except ClientError as e:
                logger.info(f'Error: {e}\n\nPossible credential refresh, sleeping...\n')
                sleep(10)
                continue
        return table
