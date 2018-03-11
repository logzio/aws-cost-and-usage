import boto3
import dateutil.relativedelta
import json
import logging
import os
import sys
import time
import urllib2

from csv import DictReader
from dateutil import parser
from io import BytesIO
from gzip import GzipFile

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MaxRetriesException(Exception):
    pass


class UnauthorizedAccessException(Exception):
    pass


class BadLogsException(Exception):
    pass


class BadConfigurationFile(Exception):
    pass


class UnknownURL(Exception):
    pass


class BulkHTTPRequest(object):
    MAX_BULK_SIZE_IN_BYTES = 1 * 1024 * 1024

    def __init__(self, logzio_url):
        self._size = 0
        self._logs = []
        self._logzio_url = logzio_url

    def add(self, log):
        # type: (dict) -> None
        json_log = json.dumps(log)
        self._logs.append(json_log)
        self._size += sys.getsizeof(json_log)
        self._try_to_send()

    def _reset(self):
        self._size = 0
        self._logs = []

    def _try_to_send(self):
        if self._size > self.MAX_BULK_SIZE_IN_BYTES:
            self._send_to_logzio()
            self._reset()

    def flush(self):
        if self._size:
            self._send_to_logzio()
            self._reset()

    @staticmethod
    def retry(func):
        def retry_func():
            max_retries = 4
            sleep_between_retries = 2

            for retries in xrange(max_retries):
                if retries:
                    sleep_between_retries *= 2
                    logger.info("Failure in sending logs - Trying again in {} seconds"
                                .format(sleep_between_retries))
                    time.sleep(sleep_between_retries)
                try:
                    res = func()
                except urllib2.HTTPError as e:
                    status_code = e.getcode()
                    if status_code == 400:
                        raise BadLogsException(e.reason)
                    elif status_code == 401:
                        raise UnauthorizedAccessException()
                    elif status_code == 404:
                        raise UnknownURL()
                    else:
                        logger.error("Unknown HTTP exception: {}".format(e))
                        continue
                except urllib2.URLError:
                    raise
                return res

            raise MaxRetriesException()

        return retry_func

    def _send_to_logzio(self):
        @BulkHTTPRequest.retry
        def do_request():
            headers = {"Content-type": "application/json"}
            request = urllib2.Request(self._logzio_url, data='\n'.join(self._logs), headers=headers)
            return urllib2.urlopen(request)

        try:
            do_request()
            logger.info("Successfully sent bulk of {} logs to Logz.io!".format(len(self._logs)))
        except MaxRetriesException:
            logger.error('Retry limit reached. Failed to send log entry.')
            raise MaxRetriesException()
        except BadLogsException as e:
            logger.error("Got 400 code from Logz.io. This means that some of your logs are too big, "
                         "or badly formatted. response: {0}".format(e.message))
            raise BadLogsException()
        except UnauthorizedAccessException:
            logger.error("You are not authorized with Logz.io! Token OK? dropping logs...")
            raise UnauthorizedAccessException()
        except UnknownURL:
            logger.error("Please check your url...")
            raise UnknownURL()
        except urllib2.HTTPError as e:
            logger.error("Unexpected error while trying to send logs: {}".format(e))
            raise


def _download_csv_object(obj):
    # type: (dict) -> str
    byte_stream = BytesIO(obj['Body'].read())
    csv_unzip = GzipFile(None, 'rb', fileobj=byte_stream).read().decode('utf-8')
    return csv_unzip


def _download_manifest_file(obj):
    # type: (dict) -> dict
    file_content = obj['Body'].read()
    json_content = json.loads(file_content)
    return json_content


def _download_object(obj, is_csv):
    return _download_csv_object(obj) if is_csv else _download_manifest_file(obj)


def _parse_file(csv_lines, logzio_url, event_time):
    # type: (list[str], str, str) -> None
    reader = DictReader(csv_lines, delimiter=',')
    reader.fieldnames = [header.replace('/', '_') for header in reader.fieldnames]
    shipper = BulkHTTPRequest(logzio_url)
    for row in reader:
        row['@timestamp'] = event_time
        row['uuid'] = "billing_report_{}".format(event_time)

        empty_keys = [k for k, v in row.iteritems() if not v]
        for k in empty_keys:
            del row[k]

        shipper.add(row)

    shipper.flush()


def _environment_variables():
    # type: () -> dict
    env_var = {
        'logzio_url': os.environ['URL'],
        'token': os.environ['TOKEN'],
        'bucket': os.environ['S3_BUCKET_NAME'],
        'report_path': os.environ['REPORT_PATH'],
        'report_name': os.environ['REPORT_NAME']
    }
    return env_var


def _latest_csv_keys(s3client, env_var, event_time):
    # type: ('boto3.client', dict, str) -> list[str]
    # example: 20180201-20180301
    start = parser.parse(event_time)
    end = start + dateutil.relativedelta.relativedelta(months=1)
    report_monthly_folder = "{:02d}{:02d}01-{:02d}{:02d}01".format(start.year, start.month, end.year, end.month)
    try:
        obj = s3client.get_object(Bucket=env_var['bucket'], Key="{0}/{1}/{2}-Manifest.json"
                                  .format(env_var['report_path'],
                                  report_monthly_folder,
                                  env_var['report_name']))

        json_content = _download_object(obj, False)
        # report can be split to a few .gz files
        return json_content['reportKeys']
    except s3client.exceptions.NoSuchKey:
            # take previous months range if today is not available
            # can happen when we change months and no new report yet
            # see issue - https://github.com/PriceBoardIn/aws-elk-billing/issues/16
            end = start
            start = end - dateutil.relativedelta.relativedelta(months=1)
            report_monthly_folder = "{:02d}{:02d}01-{:02d}{:02d}01".format(start.year, start.month, end.year, end.month)
            obj = s3client.get_object(Bucket=env_var['bucket'],
                                      Key="{0}/{1}/{2}-Manifest.json"
                                      .format(env_var['report_path'],
                                      report_monthly_folder,
                                      env_var['report_name']))

            json_content = _download_object(obj, False)
            # report can be split to a few .gz files
            return json_content["reportKeys"]


def _validate_event(event):
    # type: (dict) -> (dict, str)
    env_var = _environment_variables()
    event_time = event['time']
    return env_var, event_time


def lambda_handler(event, context):
    # type: (dict, dict) -> None
    try:
        env_var, event_time = _validate_event(event)
    except KeyError as e:
        logger.error("Unexpected event - check you set environment variables, "
                     "and that your event is scheduled correctly: {}".format(e))
        raise

    logzio_url = "{0}/?token={1}&type=billing".format(env_var['logzio_url'], env_var['token'])
    s3client = boto3.client('s3')
    try:
        latest_csv_keys = _latest_csv_keys(s3client, env_var, event_time)
    except s3client.exceptions.NoSuchKey:
        logger.error("Could not find latest report that is in the Manifest file")
        raise s3client.exceptions.NoSuchKey()

    for key in latest_csv_keys:
            csv_like_obj = s3client.get_object(Bucket=env_var['bucket'], Key=key)
            csv_obj = _download_object(csv_like_obj, True)
            csv_lines = csv_obj.splitlines(True)
            _parse_file(csv_lines, logzio_url, event_time)
