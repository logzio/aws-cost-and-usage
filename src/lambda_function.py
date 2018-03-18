import boto3
import dateutil.relativedelta
import json
import logging
import os

from csv import DictReader
from dateutil import parser
from io import BytesIO
from gzip import GzipFile
from shipper import LogzioShipper

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


def _parse_float(s):
    try:
        return float(s)
    except ValueError:
        return s


def _parse_int(s):
    try:
        return int(s)
    except ValueError:
        return s


def get_fields_parser():
    # type: (None) -> dict
    return {
        "bill_PayerAccountId": (_parse_int, int),
        "lineItem_UsageAmount": (_parse_float, float),
        "lineItem_BlendedRate": (_parse_float, float),
        "lineItem_BlendedCost": (_parse_float, float),
        "lineItem_UnBlendedRate": (_parse_float, float),
        "lineItem_UnBlendedCost": (_parse_float, float),
        "pricing_publicOnDemandCost": (_parse_float, float),
        "pricing_publicOnDemandRate": (_parse_float, float),
    }


def _parse_file(csv_lines, logzio_url, event_time):
    # type: (list[str], str, str) -> None
    reader = DictReader(csv_lines, delimiter=',')
    reader.fieldnames = [header.replace('/', '_') for header in reader.fieldnames]
    shipper = LogzioShipper(logzio_url)
    fields_parser = get_fields_parser()
    for row in reader:
        row['@timestamp'] = event_time
        row['uuid'] = "billing_report_{}".format(event_time)
        for header, tab in row.items():
            if not tab:
                logger.info("header - {} - tab - {}".format(header, tab))
                del row[header]
            elif header in fields_parser:
                row[header] = fields_parser[header][0](tab)

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
