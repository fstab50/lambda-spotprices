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
import argparse
import subprocess
import boto3
import threading
from botocore.exceptions import ClientError
from spotlib import SpotPrices, UtcConversion
from libtools import stdout_message
from libtools.js import export_iterobject
from libtools import logd
from pyaws.awslambda import read_env_variable
from _version import __version__


logger = logd.getLogger('1.0')

try:
    from libtools.oscodes_unix import exit_codes
    os_type = 'Linux'
    user_home = os.getenv('HOME')
    splitchar = '/'                                   # character for splitting paths (linux)

except Exception:
    from libtools.oscodes_win import exit_codes         # non-specific os-safe codes
    os_type = 'Windows'
    user_home = os.getenv('username')
    splitchar = '\\'                                  # character for splitting paths (windows)


# globals
container = []
module = os.path.basename(__file__)
iloc = os.path.abspath(os.path.dirname(__file__))     # installed location of modules


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
    region = bcy + key.split('/')[0] + rst
    pricefile = bcy + key.split('/')[1] + rst
    delimiter = bdwt + '/' + rst
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


def s3upload(bucket, s3object, key, profile='default'):
    """
        Streams object to S3 for long-term storage

    Returns:
        Success | Failure, TYPE: bool
    """
    try:
        session = boto3.Session(profile_name=profile)
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
        success = f'Wrote {bcy + filename + rst}\n{tab}successfully to local filesystem'
        stdout_message(success, prefix='OK')
        return True
    else:
        failure = f'Problem writing {bcy + filename + rst} to local filesystem'
        stdout_message(failure, prefix='WARN')
        return False


class AssignRegion():
    """Map AvailabilityZone to corresponding AWS region"""
    def __init__(self):
        self.client = boto3.client('ec2')
        self.regions = [x['RegionName'] for x in self.client.describe_regions()['Regions']]

    def assign_region(self, az):
        return [x for x in self.regions if x in az][0]


class DynamoDBPrices():
    def __init__(self, region, table_name, start_date, end_date):
        self.ar = AssignRegion()
        self.sp = SpotPrices(start_dt=start_date, end_dt=end_date)
        self.regions = self.ar.regions
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def load_pricedata(self, price_dicts):
        """
            Inserts data items into DynamoDB table
                - Partition Key:  Timestamp
                - Sort Key: Spot Price
        Args:
            region_list (list): AWS region code list from which to gen price data

        Returns:
            dynamodb table object

        """
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
                logger.info(f'Error inserting item {export_iterobject(item)}: \n\n{e}')
                continue
        return True


def lambda_handler():
    """
    Initialize spot price operations; process command line parameters
    """
    environment_dict = source_environment()

    # create dt object start, end datetimes
    start, end = default_endpoints()

    # set local region, dynamoDB table
    REGION = read_env_variable('REGION', 'us-east-2')
    TARGET_REGIONS = read_env_variable('TARGET_REGIONS').split(',')
    TABLE = read_env_variable('DYNAMODB_TABLE', 'PriceData')
    sp = SpotPrices()

    prices = sp.generate_pricedata(regions=TARGET_REGIONS)
    uc = UtcConversion(prices)      # converts datatime objects to str date times
    price_dicts = prices['SpotPriceHistory']

    # divide price list into multiple parts for parallel processing
    prices1, prices2 = split_list(price_dicts, 2)

    # prepare both thread facilities for dynamoDB insertion
    db1 = DynamoDBPrices(region=REGION, table_name=TABLE, start_date=start, end_date=end)
    db2 = DynamoDBPrices(region=REGION, table_name=TABLE, start_date=start, end_date=end)

    # retrieve spot data, insert into dynamodb
    pb_thread1 = db1.load_pricedata(prices1)
    pb_thread1.start()
    pb_thread2 = db2.load_pricedata(prices2)
    pb_thread2.start()

    while True:
        pb_thread1.stop() if pb_thread1 else continue
        pb_thread2.stop() if pb_thread2 else continue

    sys.exit(exit_codes['E_BADARG']['Code'])

    fname = '_'.join(
                [
                    start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'all-instance-spot-prices.json'
                ]
            )

    # write to file on local filesystem
    key = os.path.join(region, fname)
    os.makedirs(region) if not os.path.exists(region) else True
    _completed = export_iterobject(prices, key)

    # log status
    tab = '\t'.expandtabs(13)
    fkey = format_pricefile(key)
    success = f'Wrote {fkey}\n{tab}successfully to local filesystem'
    failure = f'Problem writing {fkey} to local filesystem'
    stdout_message(success, prefix='OK') if _completed else stdout_message(failure, prefix='WARN')

    # build unique collection of instances for this region
    regional_sizes = list(set([x['InstanceType'] for x in prices['SpotPriceHistory']]))
    instance_sizes.extend(regional_sizes)

    # instance sizes across analyzed regions
    instance_sizes = list(set(instance_sizes))
    instance_sizes.sort()
    key = 'instanceTypes'
    date = sp.end.strftime("%Y-%m-%d")
    return writeout_data(key, instance_sizes, date + '_spot-instanceTypes.json')

    failure = """ : Check of runtime parameters failed for unknown reason.
    Please ensure you have both read and write access to local filesystem. """
    logger.warning(failure + 'Exit. Code: %s' % sys.exit(exit_codes['E_MISC']['Code']))
    print(failure)
    return sys.exit(exit_codes['E_BADARG']['Code'])
