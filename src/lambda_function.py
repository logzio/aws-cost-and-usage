import boto3
import dateutil.relativedelta
import json
import logging
import os
import sys
import urllib2

from csv import DictReader
from dateutil import parser
from io import BytesIO
from gzip import GzipFile

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_BULK_SIZE_IN_BYTES = 1 * 1024 * 1024


class MaxRetriesException(Exception):
    pass


class UnauthorizedAccessException(Exception):
    pass


class BadLogsException(Exception):
    pass


class BadConfigurationFile(Exception):
    pass


def retry_send(func):
    def retry_func():
        max_retries = 4
        sleep_between_retries = 2

        for i in xrange(max_retries):
            try:
                response = func()
            except urllib2.HTTPError as e:
                status_code = e.getcode()
                logger.error("Unexpected exception while trying to send: {}".format(e.reason))
                if status_code == 400:
                    raise BadLogsException(e.reason)
                elif status_code == 401:
                    raise UnauthorizedAccessException()
            except urllib2.URLError:
                sleep_between_retries *= 2
                logger.info("Failure is retriable - Trying again in {} seconds".format(sleep_between_retries))
            else:
                return response

        raise MaxRetriesException()

    return retry_func


def _send_to_logzio(parsed_json_rows, logzio_url):
    @retry_send
    def do_request():
        headers = {"Content-type": "application/json"}
        request = urllib2.Request(logzio_url, data='\n'.join(parsed_json_rows), headers=headers)
        return urllib2.urlopen(request)

    try:
        response = do_request()
        logger.info("Successfully sent bulk of {} logs to Logz.io!".format(len(parsed_json_rows)))
    except MaxRetriesException:
        logger.error('Retry limit reached. Failed to send log entry.')
    except BadLogsException as e:
        logger.error("Got 400 code from Logz.io. This means that some of your logs are too big, "
                     "or badly formatted. response: {0}".format(e.message))
    except UnauthorizedAccessException:
        logger.error("You are not authorized with Logz.io! Token OK? dropping logs...")


def _download_csv_object(obj):
    byte_stream = BytesIO(obj['Body'].read())
    csv_unzip = GzipFile(None, 'rb', fileobj=byte_stream).read().decode('utf-8')
    return csv_unzip


def _download_manifest_file(obj):
    file_content = obj['Body'].read()
    json_content = json.loads(file_content)
    return json_content


def _download_object(obj, is_csv):
    if is_csv:
        return _download_csv_object(obj)
    # json  manifest file
    else:
        return _download_manifest_file(obj)


def _parse_file(csv_lines, logzio_url, event_time):
    reader = DictReader(csv_lines, delimiter=',')
    reader.fieldnames = [header.replace('/', '_') for header in reader.fieldnames]
    parsed_json_rows = []
    current_total_size = 0
    for row in reader:
        row['@timestamp'] = event_time
        row['uuid'] = "billing_report_{}".format(event_time)

        empty_keys = [k for k, v in row.iteritems() if not v]
        for k in empty_keys:
            del row[k]

        json_dic = json.dumps(row)
        parsed_json_rows.append(json_dic)
        current_total_size += sys.getsizeof(json_dic)
        if current_total_size >= MAX_BULK_SIZE_IN_BYTES:
            _send_to_logzio(parsed_json_rows, logzio_url)
            parsed_json_rows = []
            current_total_size = 0

    if parsed_json_rows:
        _send_to_logzio(parsed_json_rows, logzio_url)


def _environment_variables():
    env_var = {
        'logzio_url': os.environ['URL'],
        'token': os.environ['TOKEN'],
        'bucket': os.environ['S3_BUCKET_NAME'],
        'report_path': os.environ['REPORT_PATH'],
        'report_name': os.environ['REPORT_NAME']
    }
    return env_var


def _latest_csv_keys(s3client, env_var, event_time):
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
        return json_content["reportKeys"]
    except s3client.exceptions.NoSuchKey as e:
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

    return None


def _validate_event(event):
    env_var = _environment_variables()
    event_time = event['time']
    return env_var, event_time


def lambda_handler(event, context):
    try:
        env_var, event_time = _validate_event(event)
    except KeyError as e:
        logger.error("Unexpected event - check you set environment variables, "
                     "and that your event is scheduled correctly: {}".format(e))
        raise

    logzio_url = "{0}/?token={1}&type=billing".format(env_var['logzio_url'], env_var['token'])
    s3client = boto3.client('s3')

    latest_csv_keys = _latest_csv_keys(s3client, env_var, event_time)
    if latest_csv_keys is None:
        logger.error("Could not find Manifest.json - please check your bucket")
        return

    for key in latest_csv_keys:
            csv_like_obj = s3client.get_object(Bucket=env_var['bucket'], Key=key)
            csv_obj = _download_object(csv_like_obj, True)
            csv_lines = csv_obj.splitlines(True)
            _parse_file(csv_lines, logzio_url, event_time)
