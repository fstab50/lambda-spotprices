"""

lambda EC2 spotprice retriever, GPL v3 License

Copyright (c) 2018-2019 Blake Huber

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""
import os
import sys
import datetime
import json
import inspect
import subprocess
import boto3
import threading
from botocore.exceptions import ClientError
from spotlib import SpotPrices, UtcConversion
from libtools.js import export_iterobject
from libtools.oscodes_unix import exit_codes
from pyaws.awslambda import read_env_variable
import loggers
from _version import __version__

logger = loggers.getLogger(__version__)

# globals
module = os.path.basename(__file__)


def _debug_output(*args):
    """additional verbose information output"""
    for arg in args:
        if os.path.isfile(arg):
            print('Filename {}'.format(arg.strip(), 'lower'))
        elif str(arg):
            print('String {} = {}'.format(getattr(arg.strip(), 'title'), arg))


def _get_regions():
    client = boto3.client('ec2')
    return [x['RegionName'] for x in client.describe_regions()['Regions']]


def standardize_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def utc_datetime(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def datetimify_standard(s):
    """Function to create timezone unaware string"""
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


def default_endpoints(duration_days=1):
    """
    Supplies the default start and end datetime objects in absence
    of user supplied endpoints which frames time period from which
    to begin and end retrieving spot price data from Amazon APIs.

    Returns:  TYPE: tuple, containing:
        - start (datetime), midnight yesterday
        - end (datetime) midnight, current day

    """
    # end datetime calcs
    dt_date = datetime.datetime.today().date()
    dt_time = datetime.datetime.min.time()
    end = datetime.datetime.combine(dt_date, dt_time)

    # start datetime calcs
    duration = datetime.timedelta(days=duration_days)
    start = end - duration
    return start, end


def format_pricefile(key):
    """Adds path delimiter and color formatting to output artifacts"""
    region = key.split('/')[0]
    pricefile = key.split('/')[1]
    delimiter = '/'
    return region + delimiter + pricefile


def summary_statistics(data, instances):
    """
    Calculate stats across spot price data elements retrieved
    in the current execution.  Prints to stdout

    Args:
        :data (list): list of spot price dictionaries
        :instances (list): list of unique instance types found in data

    Returns:
        Success | Failure, TYPE:  bool
    """
    instance_dict, container = {}, []

    for itype in instances:
        try:
            cur_type = [
                x['SpotPrice'] for x in data['SpotPriceHistory'] if x['InstanceType'] == itype
            ]
        except KeyError as e:
            logger.exception('KeyError on key {} while printing summary report statistics.'.format(e))
            continue

        instance_dict['InstanceType'] = str(itype)
        instance_dict['AvgPrice'] = sum([float(x['SpotPrice']) for x in cur_type]) / len(cur_type)
        container.append(instance_dict)
    # output to stdout
    print_ending_summary(instances, container)
    return True


def print_ending_summary(itypes_list, summary_data):
    """
    Prints summary statics to stdout at the conclusion of spot
    price data retrieval
    """
    now = datetime.datetime.now().strftime('%Y-%d-%m %H:%M:%S')
    tab = '\t'.expandtabs(4)
    print('EC2 Spot price data retrieval concluded {}'.format(now))
    print('Found {} unique EC2 size types in spot data'.format(len(itypes_list)))
    print('Instance Type distribution:')
    for itype in itypes_list:
        for instance in container:
            if instance['InstanceType'] == itype:
                print('{} - {}'.format(tab, itype, instance['AvgPrice']))


def source_environment(env_variable):
    """
    Sources all environment variables
    """
    return {
        'duration_days': read_env_variable('DEFAULT_DURATION'),
        'page_size': read_env_variable('PAGE_SIZE', 700),
        'bucket': read_env_variable('S3_BUCKET', None)
    }.get(env_variable, None)


def s3upload(bucket, s3object, key):
    """
        Streams object to S3 for long-term storage

    Returns:
        Success | Failure, TYPE: bool
    """
    try:
        session = boto3.Session()
        s3client = session.client('s3')
        # dict --> str -->  bytes (utf-8 encoded)
        bcontainer = json.dumps(s3object, indent=4, default=str).encode('utf-8')
        response = s3client.put_object(Bucket=bucket, Body=bcontainer, Key=key)

        # http completion code
        statuscode = response['ResponseMetadata']['HTTPStatusCode']

    except ClientError as e:
        logger.exception(f'Unknown exception while calc start & end duration: {e}')
        return False
    return True if str(statuscode).startswith('20') else False


def split_list(mlist, n):
    """
    Summary.

        splits a list into equal parts as allowed, given n segments

    Args:
        :mlist (list):  a single list containing multiple elements
        :n (int):  Number of segments in which to split the list

    Returns:
        generator object

    """
    k, m = divmod(len(mlist), n)
    return (mlist[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def writeout_data(key, jsonobject, filename):
    """
        Persists json data to local filesystem

    Returns:
        Success | Failure, TYPE: bool

    """
    tab = '\t'.expandtabs(13)

    if export_iterobject({key: jsonobject}, filename):
        success = f'Wrote {filename}\n{tab}successfully to local filesystem'
        logger.info(success)
        return True
    else:
        failure = f'Problem writing {filename} to local filesystem'
        logger.warning(failure)
        return False


class AssignRegion():
    """Map AvailabilityZone to corresponding AWS region"""
    def __init__(self):
        self.client = boto3.client('ec2')
        self.regions = [x['RegionName'] for x in self.client.describe_regions()['Regions']]

    def assign_region(self, az):
        return [x for x in self.regions if x in az][0]


class DynamoDBPrices(threading.Thread):
    def __init__(self, region, table_name, price_dicts, start_date, end_date):
        super(DynamoDBPrices, self).__init__()
        self.ar = AssignRegion()
        self.sp = SpotPrices(start_dt=start_date, end_dt=end_date)
        self.regions = self.ar.regions
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.prices = price_dicts
        self.running = False

    def start(self):
        self.running = True
        super(DynamoDBPrices, self).start()

    def run(self):
        """
            Inserts data items into DynamoDB table
                - Partition Key:  Timestamp
                - Sort Key: Spot Price
        Args:
            region_list (list): AWS region code list from which to gen price data

        Returns:
            dynamodb table object

        """
        for item in self.prices:
            try:
                self.table.put_item(
                    Item={
                            'RegionName':  self.ar.assign_region(item['AvailabilityZone']),
                            'AvailabilityZone': item['AvailabilityZone'],
                            'InstanceType': item['InstanceType'],
                            'ProductDescription': item['ProductDescription'],
                            'SpotPrice': item['SpotPrice'],
                            'Timestamp': item['Timestamp'],
                            'OnDemandPrice': {"USD": "0.0000000000", 'unit': 'Hrs'}
                    }
                )
                logger.info(
                    'Successful put item for AZ {} at time {}'.format(item['AvailabilityZone'], item['Timestamp'])
                )
                if not self.running:
                    break
            except ClientError as e:
                logger.info(f'Error inserting item {item}: \n\n{e}')
                continue

    def stop(self):
        self.running = False
        self.join()  # wait for run() method to terminate
        sys.stdout.flush()


def download_spotprice_data(region_list):
    sp = SpotPrices()
    prices = sp.generate_pricedata(regions=region_list)
    uc = UtcConversion(prices)      # converts datatime objects to str date times
    return prices['SpotPriceHistory']


def set_tempdirectory():
    TMPDIR = '/tmp'
    os.environ['TMPDIR'] = TMPDIR
    os.environ['TMP'] = TMPDIR
    os.environ['TEMP'] = TMPDIR
    subprocess.getoutput('export TMPDIR=/tmp')


def lambda_handler(event, context):
    """
    Initialize spot price operations; process command line parameters
    """
    # change to writeable filesystem
    os.chdir('/tmp')
    logger.info('PWD is {}'.format(os.getcwd()))

    set_tempdirectory()

    # create dt object start, end datetimes
    start, end = default_endpoints()

    # set local region, dynamoDB table
    REGION = read_env_variable('DEFAULT_REGION', 'us-east-2')
    TARGET_REGIONS = read_env_variable('TARGET_REGIONS').split(',')
    TABLE = read_env_variable('DYNAMODB_TABLE', 'PriceData')
    BUCKET = read_env_variable('S3_BUCKET')

    # log status
    logger.info('Environment variable status:')
    logger.info('REGION: {}'.format(REGION))
    logger.info('TARGET_REGIONS: {}'.format(TARGET_REGIONS))
    logger.info('TABLAKE: {}'.format(TABLE))
    logger.info('BUCKET: {}'.format(BUCKET))

    price_list = download_spotprice_data(TARGET_REGIONS)

    # divide price list into multiple parts for parallel processing
    prices1, prices2, prices3, prices4 = split_list(price_list, 4)

    logger.info('prices1 contains: {} elements'.format(len(prices1)))
    logger.info('prices2 contains: {} elements'.format(len(prices2)))
    logger.info('prices3 contains: {} elements'.format(len(prices3)))
    logger.info('prices4 contains: {} elements'.format(len(prices4)))

    # prepare both thread facilities for dynamoDB insertion
    db1 = DynamoDBPrices(region=REGION, table_name=TABLE, price_dicts=prices1, start_date=start, end_date=end)
    db2 = DynamoDBPrices(region=REGION, table_name=TABLE, price_dicts=prices2, start_date=start, end_date=end)
    db3 = DynamoDBPrices(region=REGION, table_name=TABLE, price_dicts=prices3, start_date=start, end_date=end)
    db4 = DynamoDBPrices(region=REGION, table_name=TABLE, price_dicts=prices4, start_date=start, end_date=end)

    # retrieve spot data, insert into dynamodb
    db1.start()
    db2.start()
    db3.start()
    db4.start()

    # need to join, concurrent end to all threads
    db1.join()
    db2.join()
    db3.join()
    db4.join()

    # save raw data in Amazon S3, one file per region
    for region in TARGET_REGIONS:

        price_list = download_spotprice_data([region])

        fname = '_'.join(
                    [
                        start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'all-instance-spot-prices.json'
                    ]
                )

        # write to file on local filesystem
        key = os.path.join(region, fname)
        _completed = s3upload(BUCKET, {'SpotPriceHistory': price_list}, key)
        logger.info('Completed upload to Amazon S3 for region {}'.format(region))

        # log status
        tab = '\t'.expandtabs(13)
        fkey = format_pricefile(key)
        success = f'Wrote {fkey}\n{tab}successfully to local filesystem'
        failure = f'Problem writing {fkey} to local filesystem'
        logger.info(success) if _completed else logger.warning(failure)

    return True
