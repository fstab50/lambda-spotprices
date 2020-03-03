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
from botocore.exceptions import ClientError
from libtools import stdout_message
from libtools.js import export_iterobject
from spotlib import SpotPrices, UtcConversion
from spotlib.help_menu import menu_body
from spotlib import about, logger
from spotlib.variables import acct, bdwt, bbc, bbl, bcy, btext, rst


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


def help_menu():
    """Print help menu options"""
    print(menu_body)


def local_awsregion(profile):
    """Determines AWS region code local to user"""
    if os.environ.get('AWS_DEFAULT_REGION'):
        return os.environ['AWS_DEFAULT_REGION']
    cmd = 'aws configure get {}.region'.format(profile)
    return subprocess.getoutput(cmd).strip()


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
        'duration_days': read_env_variable('default_duration'),
        'page_size': read_env_variable('page_size', 500),
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



def lambda_handler():
    """
    Initialize spot price operations; process command line parameters
    """

    if (args.start and args.end) or args.duration:
        # set local region
        args.region = local_awsregion(args.profile) if args.region == 'noregion' else args.region

        sp = SpotPrices(profile=args.profile)

        if args.duration and isinstance(int(args.duration[0]), int):
            start, end = sp.set_endpoints(duration=int(args.duration[0]))
        else:
            start, end = sp.set_endpoints(args.start, args.end)

        # global container for ec2 instance size types
        instance_sizes = []

        for region in args.region:

            fname = '_'.join(
                        [
                            start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'all-instance-spot-prices.json'
                        ]
                    )

            prices = sp.generate_pricedata(regions=[region])

            # conversion of datetime obj => utc strings
            uc = UtcConversion(prices)

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

    else:
        stdout_message(
            'Dependency check fail %s' % json.dumps(args, indent=4),
            prefix='AUTH',
            severity='WARNING'
            )
        sys.exit(exit_codes['E_DEPENDENCY']['Code'])

    failure = """ : Check of runtime parameters failed for unknown reason.
    Please ensure you have both read and write access to local filesystem. """
    logger.warning(failure + 'Exit. Code: %s' % sys.exit(exit_codes['E_MISC']['Code']))
    print(failure)
    return sys.exit(exit_codes['E_BADARG']['Code'])
