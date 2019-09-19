import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from functools import lru_cache
from multiprocessing.dummy import Pool
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector
from pyaws.AWSLambda import read_env_variable


# globals
REGION = read_env_variable('REGION', default='eu-west-1')
DB = boto3.resource('dynamodb', region_name=REGION)

METADATA_TABLENAME = read_env_variable('METADATA_TABLENAME', default='MPCAWS_EC2_METADATA')
METADATA_TABLE = DB.Table(METADATA_TABLENAME)

#EC2PRICE_TABLENAME = read_env_variable('EC2PRICE_TABLENAME', default='MPCAWS_EC2_PRICETABLE')
#EC2PRICE_TABLE = DB.Table(EC2_TABLENAME)

DB_CLIENT = boto3.client('dynamodb', region_name=REGION)


def standardize_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


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


def _write_expressions(exp_tuple, table_object):
    update_key, expression_values = exp_tuple
    table_object.update_item(
        Key=update_key,
        UpdateExpression='set backup_type=:type, policy=:policy, backup_status=:status, stop_time=:stop, run_region=:region',
        ExpressionAttributeValues=expression_values,
        ReturnValues='UPDATED_OLD'
    )


def _create_expressions(dataset):
    for customer in dataset:
        for l1 in dataset[customer]:
            for l2 in dataset[customer][l1]:
                update_key = {
                    'customer|tool': '|'.join((
                        customer, dataset[customer][l1][l2].get('Tool', 'unknown')
                    ))
                }
                for date, detail_list in dataset[customer][l1][l2]['dates'].items():
                    for detail in detail_list:
                        update_key['start|l1|l2|bckid'] = '|'.join((
                            standardize_datetime(detail['Start']),
                            l1,
                            l2,
                            str(detail.get('Id', '-1'))
                        ))
                        # print(update_key['start|l1|l2|bckid'])
                        yield (update_key.copy(), {
                            ':type': dataset[customer][l1][l2]['Type'],
                            ':policy': dataset[customer][l1][l2]['Policy'],
                            ':status': detail['Status'],
                            ':stop': standardize_datetime(detail['Stop']),
                            ':region': REGION
                        })


def push_standardized(dataset):
    exps = tuple(_create_expressions(dataset))

    with Pool(8) as p:
        p.map(_write_expressions, exps)
