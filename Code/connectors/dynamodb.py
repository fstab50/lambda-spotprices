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


def response_generator(client, operation_parameters):
    service_model = client._service_model.operation_model('Query')
    trans = TransformationInjector(deserializer = TypeDeserializer())
    response = client.query(**operation_parameters)
    more_items = True
    while more_items:
        if 'LastEvaluatedKey' in response:
            operation_parameters['ExclusiveStartKey'] = response['LastEvaluatedKey'].copy()
            trans.inject_attribute_value_output(response, service_model)
            for item in response['Items']:
                yield item
            response = client.query(**operation_parameters)
        else:
            more_items = False
            trans.inject_attribute_value_output(response, service_model)
            for item in response['Items']:
                yield item


def fetch_standardized(
    api_key=None,
    base_url=None,
    from_time: datetime=None,
    to_time: datetime=None,
    customer=None,
    backup_tool=None,
    region=None,
    *args,
    **kwargs):
    if any(x is None for x in (customer,backup_tool)):
        raise ValueError("customer and backup_tool must both be provided for the DynamoDB connector")


    query_str = "#x = :x"

    if all(x for x in (from_time, to_time)):
        query_str += " and #y between :y and :z"
    elif from_time:
        query_str += " and #y >= :y"
    elif to_time:
        query_str += " and #y <= :z"

    operation_parameters = {
      'TableName': METADATA_TABLENAME,
      'KeyConditionExpression': query_str,
      'ExpressionAttributeValues': {
        ':x': {'S': '|'.join((customer, backup_tool))},
        ':y': {'S': standardize_datetime(from_time)},
        ':z': {'S': standardize_datetime(to_time)}
      },
      'ExpressionAttributeNames': {
        "#x":"customer|tool", "#y":"start|l1|l2|bckid"
      }
    }

    out = {}
    if response_generator(DB_CLIENT, operation_parameters):
        out[customer] = {}

    for item in response_generator(DB_CLIENT, operation_parameters):
        if region is not None and item['run_region'] != region:
            continue
        start_time, l1, l2, backup_id = item['start|l1|l2|bckid'].split('|')
        l1_dict = out[customer].setdefault(l1, {
            '_cloudstatus': get_hostnames(customer, region).get(l1, 'unknown')
        })
        l2_dict = l1_dict.setdefault(l2, {
            'Policy': item['policy'],
            'Type': item['backup_type'],
            'Status': item['backup_status'],
            'Last Start': datetimify_standard(start_time),
            'Last Stop': datetimify_standard(item['stop_time']),
            'dates': {}
        })
        l2_dict['Last Start'] = max(l2_dict['Last Start'], datetimify_standard(start_time))
        l2_dict['Last Stop'] = max(l2_dict['Last Start'], datetimify_standard(item['stop_time']))
        l2_dict['Status'] = l2_dict['Status'] if item['backup_status'] == 'Success' else item['backup_status']
        dates_list = l2_dict['dates'].setdefault(datetimify_standard(start_time).date(), [])

        dates_list.append({
            'Start': datetimify_standard(start_time),
            'Stop': datetimify_standard(item['stop_time']),
            'Status': item['backup_status'],
            'Id': backup_id
        })
    if customer in ACCOUNTS:
        [out.update(fetch_standardized(
                api_key=api_key,
                base_url=base_url,
                from_time=from_time,
                to_time=to_time,
                customer=x,
                backup_tool=backup_tool
            ))
            for x in sorted(ACCOUNTS[customer])
        ]
    return out


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


@lru_cache()
def get_not_backed(customer, machines, region=None):
    in_cloud = set(get_hostnames(customer, region).keys())
    in_dataset = set(machines)
    return in_cloud - in_dataset


def annotate_l1(dataset, region=None):
    for customer in dataset:
        not_backed = get_not_backed(customer, tuple(sorted(dataset[customer].keys())), region)
        for l1 in dataset[customer]:
            cloudstatus = get_hostnames(customer, region).get(l1, 'unknown')
            dataset[customer][l1]['_cloudstatus'] = cloudstatus
            if cloudstatus != 'unknown':
                if l1 in not_backed:
                    dataset[customer][l1]['_backupstatus'] = 'no backup'
                else:
                    dataset[customer][l1]['_backupstatus'] = 'backup'
            else:
                dataset[customer][l1]['_backupstatus'] = 'n/a'
        for server in not_backed:
            server_data = dataset[customer].setdefault(server, {})
            server_data['_backupstatus'] = 'no backup'

    return dataset
