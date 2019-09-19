"""
Summary.

    EC2 Price API Retrieval

Args:

Returns:
    Success | Failure, TYPE: bool

"""
import os
import sys
import json
import inspect
import requests
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta
from functools import lru_cache
from multiprocessing.dummy import Pool
import pytz
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from core.dynamodb import METADATA_TABLE
from pyaws.AWSLambda import read_env_variable
from pyaws.utils import TimeDelta
from pyaws import Colors
from core import ParameterSet
from core import logger



# global objects
INDEXURL = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json'
ACCENT = Colors.BOLD + Colors.BRIGHTWHITE
RESET = Colors.RESET
TMPDIR = '/tmp'
DBUGMODE = read_env_variable('DBUGMODE')

_today = datetime.utcnow()


def get_service_url(service, url=INDEXURL):
    """
    Summary.

        Retrieve Amazon API Global Offer File (Service API Index) File

    Args:
        :url (str): universal resource locator for Amazon API Index file.
            index file details the current url locations for retrieving the
            most up to date API data files
    Returns:
        Current URL of EC2 Price file (str), Publication date (str)

    """
    url_prefix = 'https://pricing.us-east-1.amazonaws.com'
    converted_name = name_lookup(service)

    if not converted_name:
        logger.critical(
            f'{inspect.stack()[0][3]}: The boto3 service name provided could \
            not be found in the index file')
        return None

    r = requests.get(url)
    f1 = json.loads(r.content)
    index_url = url_prefix + f1['offers'][converted_name]['currentRegionIndexUrl']
    data = json.loads(requests.get(index_url).content)
    url_suffix = data['regions']['us-east-1']['currentVersionUrl']
    return url_prefix + url_suffix


def publication_date(service_url):
    """
    Summary.

        Returns Publication Date of latest Amazon Price API
        File for a Named, Specific AWS Service (EC2, S3, etc)

    Args:
        :service_url (str): universal resource locator for Amazon API Price
            data file associated with a specific AWS Service (EC2, S3, etc)
            Price file details the current price per unit time of all on-demand
            resources in the AWS environment
    Returns:
        Publication date (datetime object): date of AWS Service API Price Data

    """
    def convert_datestring(string):
        return datetime.strptime(string, '%Y-%m-%dT%H:%M:%SZ')

    file_path = TMPDIR + '/' + 'index.json'

    try:

        if not os.path.exists(file_path):
            path = urllib.request.urlretrieve(service_url, file_path)[0]
        else:
            path = file_path

        if os.path.exists(file_path) and path:
            with open(path) as f1:
                data = json.loads(f1.read())
    except KeyError as e:
        logger.exception(f'{inspect.stack()[0][3]}: KeyError occured: {e}')
    except urllib.error.HTTPError as e:
        logger.exception(
            '%s: Failed to retrive file object: %s. Exception: %s, data: %s' %
            (inspect.stack()[0][3], file_path, str(e), e.read()))
        raise e
    return convert_datestring(data['publicationDate'])


def name_lookup(service, url=INDEXURL):
    """Summary.

        Lookup Table to convert boto3 Amazon Service names to Amazon index file names

    Args:
        :service (str): boto service descriptor (s3, ec2, sqs, etc)
        :url (str): universal resource locator for Amazon API Index file.
            index file details the current url locations for retrieving the
            most up to date API data files
    Returns:
        Corrected Service Name, TYPE (str), None if not found

    """
    key = None

    r = requests.get(url)

    try:
        for key in [x for x in json.loads(r.content)['offers']]:
            if (service.upper() or service.title()) in key:
                return key
    except KeyError as e:
        logger.exception(f'{inspect.stack()[0][3]}: KeyError while converting index keys: {e}')
    return None


def retrieve_raw_data(service_url):
    """
    Summary.

        Retrieve url of current ec2 price file

    Args:
        :service_url (str): universal resource locator for Amazon API Index file.
            index file details the current url locations for retrieving the
            most up to date API data files

    Returns:
        :data (json):  ec2 price api parsed data in json format

    """
    file_path = TMPDIR + '/' + 'index.json'

    try:
        if not os.path.exists(file_path):
            path = urllib.request.urlretrieve(service_url, file_path)[0]
        else:
            path = file_path

        if os.path.exists(file_path) and path:
            with open(path) as f1:
                data = json.loads(f1.read())
        else:
            return None
    except urllib.error.HTTPError as e:
        logger.exception(
            '%s: Failed to retrive file object: %s. Exception: %s, data: %s' %
            (inspect.stack()[0][3], file_path, str(e), e.read()))
        raise e
    return data


def process_metadata(rawdata: dict):
    """
    Summary.

        Process price file raw json schema

    Args:
        :raw (dict):  json data extracted from price file
    Returns:
        :refined (dict): refactored service product data

    """
    try:
        if rawdata.get('products') and rawdata.get('publicationDate'):
            param_obj = ParameterSet(rawdata)
            return param_obj.create()
    except Exception as e:
        logger.exception(
            '%s: Error while processing metadata in raw format: %s' %
            (inspect.stack()[0][3], str(e))
        )
    return None


def get_data_generator():
    for src, key, url in zip(DATA_SOURCES, API_KEYS, BASE_URLS):
        fetcher = get_fetcher(src)
        yield fetcher(
            api_key=key,
            base_url=url,
            from_time=START_DATE,
            to_time=END_DATE,
            customer=customer,
            backup_tool=BACKUP_TOOL,
            region=DATA_REGION
        )


@lru_cache()
def get_data():
    return tuple(get_data_generator())


def importer_lambda(*args, **kwargs):
    for data in get_data():
        for dest in DATA_TARGETS:
            pusher = get_pusher(dest)
            pusher(data)


def new_release(dt):
    """
    Summary.

        Compares pubication datetime of current file from ec2 price
        API vs. most recent publication datetime in dynamodb

    Args:
        :dt (datetime object):  publication date of data extracted from EC2 price API
    """
    try:
        response = METADATA_TABLE.query(
            IndexName='publicationDate',
            KeyConditionExpression=Key('publicationDate').eq(dt.isoformat())
        )
        items = response['Items']

        if not items:
            return True
    except ClientError as e:
        logger.critical(
            "%s: unknown problem retrieving last publicationDate (Code: %s Message: %s)" %
            (inspect.stack()[0][3], e.response['Error']['Code'],
            e.response['Error']['Message']))
    return False


def dynamodb_write(*args):
    """
    Summary.

        Writes list of json objects to dynamodb
    """
    item_ct = 0

    try:
        for record in args:
            item_frame = {}          # temp item dictionary formatter

            for key, value in record.items():
                if key in ('sku', 'publicationDate', 'size', 'productFamily'):
                    item_frame[key] = value
                elif key == 'attributes':
                    item_frame[key] = value
                    for k, v in value.items():
                        item_frame[k] = v
            item_ct += 1
            item_frame['createDateTime'] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()

            # write item to dynamodb
            METADATA_TABLE.put_item(Item=item_frame)

            logger.info(
                '%s: success: wrote new item |  item sequence: %s  |  HASH: %s' %
                (inspect.stack()[0][3], str(item_ct), str(item_frame['sku'])))
    except ClientError as e:
        logger.critical(
            "%s: Unknown problem writting items to dynamodb (Code: %s Message: %s)" %
            (inspect.stack()[0][3], e.response['Error']['Code'], e.response['Error']['Message']))
        return False
    return True


def split_list(monolith, n):
    """
    Summary.

        splits a list into equal parts as allowed, given n segments

    Args:
        :monolith (list):  a single list containing multiple elements
        :n (int):  Number of segments in which to split the list

    Returns:
        generator object

    """
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def lambda_handler(event, context):
    """
    Summary.

        Retrieves most recent price data for Amazon EC2 Service and inserts
        into DynamoDB

    Trigger:
        CloudWatch Rule

    Returns:
        Success | Failure, TYPE: bool

    """
    if DBUGMODE:
        print('Received event: \n' + json.dumps(event, indent=2))

    try:

        url = get_service_url('ec2')
        logger.info('URL returned for EC2 Price API data pull: {}'.format(url))

        # retrieve datetime object for publication date of current API data
        release_dt = publication_date(url)

        if new_release(release_dt):

            # check release_dt against most recent release datetime in dynamodDB
            # if same date, exit
            # if more recent date, proceed to process new price data

            # retrieve raw API Price data
            data = retrieve_raw_data(url)

            # extract, and transform raw API Price data
            container = process_metadata(data)

            if container:

                jobstart = datetime.utcnow()
                logger.info('begin metadata load into dynamoDB')
                logger.info(f'EC2 METADATA_TABLE starting item count: {METADATA_TABLE.item_count}')
                # --- run with concurrency ---

                # run instance of main with each item set in separate thread
                # Future: Needs a return status from pool object for each process
                pool_args = [container]

                with Pool(processes=8) as pool:
                    pool.starmap(dynamodb_write, pool_args)

                jobend = datetime.utcnow()
                duration = TimeDelta(jobend - jobstart)
                table_items = METADATA_TABLE.item_count

                logger.info(
                    'EC2 Metadata load completed. Job duration: %s minutes, %s seconds' %
                    (duration.minutes, duration.seconds))
                logger.info(f'EC2 METADATA_TABLE ending item count: {table_items}')
        else:
            logger.info(
                    'Skipping metadata pull -- lastest data (%s)already exists in dyanamoDB. Exit.' %
                    release_dt.isoformat())

    except ClientError as e:
        logger.exception(
            "%s: Unknown problem accessing bucket or sqs queue (Code: %s Message: %s)" %
            (inspect.stack()[0][3], e.response['Error']['Code'], e.response['Error']['Message']))
        return False
    except Exception as e:
        logger.exception(
            "%s: Unknown exception calling SQSForwarder (Error: %s)" %
            (inspect.stack()[0][3], str(e))
            )
        return False
    return True
