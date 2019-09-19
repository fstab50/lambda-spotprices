"""
Module for various connectors generating report data

This module is divided into sub-modules, each name corresponding to the relevant technology they're interfacing with. For instance, `connectors.cpm` connects to CPM; `connectors.networker` - to Networker, and so on.

Each of the modules provides, at the very least, the standard functions:

fetch_standardized(api_key: str, base_url: str, from_time: datetime=None, to_time: datetime=None)
=================================================================================================

This function fetches data, and returns it formatted according to the following spec (shown below in YAML notation):

```yaml

account_name:
  level_1_identifier:
    level_2_identifier:
      Last Start: <datetime object>
      Last Stop: <datetime object>
      Policy: <string>
      Type: <string>
      Tool: <string>
      Identifier: <string; optional>
      dates:
        <date object>:
          - Start: <datetime object>
            Stop: <datetime object>
            Status: Success|Error|N/A
            Sub-Identifier: <string; optional>
```

Optionally, within level_2_identifier's namespace, you can define metadata. Every field that begins with an underscore will be treated as optional metadata (such as: server's status in the cloud provider)

push_standardized(data)
=======================

This function consumes data in the above format, and pushes it to appropriate data target.
"""
import logging
from . import dynamodb

logger = logging.getLogger()

fetchers = {
  'dynamodb': dynamodb.fetch_standardized
}

pushers = {
  'dynamodb': dynamodb.push_standardized
}


def get_fetcher(name):
    def _nullfetcher(*args, **kwargs):
        logger.error("There is no fetching connector '%s', lambda is likely misconfigured. Nothing was fetched", name)
        return {}
    return fetchers.get(name, _nullfetcher)


def get_pusher(name):
    def _nullpusher(*args, **kwargs):
        logger.error("There is no fetching connector '%s', lambda is likely misconfigured. Nothing will be pushed", name)
    return pushers.get(name, _nullpusher)
