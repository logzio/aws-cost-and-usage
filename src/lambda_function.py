import boto3
import csv
import dateutil.relativedelta
import json
import logging
import os
import zlib

from dateutil import parser
from .shipper import LogzioShipper

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CSVLineGenerator(object):
    def __init__(self, csv_like_obj_body, line_delimiter='\n'):
        self._obj_body = csv_like_obj_body
        self._line_delimiter = line_delimiter
        self._dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._buff = ''
        self.headers = next(self.stream_line()).replace('/', '_')

    def stream_line(self):
        # type: (CSVLineGenerator) -> 'Generator'

        def _get_next_line():
            # search for new line
            endline_idx = self._buff.index(self._line_delimiter)
            next_line = self._buff[:endline_idx]
            self._buff = self._buff[endline_idx + 1:]
            return next_line

        def reader(stream):
            while True:
                try:
                    yield _get_next_line()
                    continue
                # no new line
                except ValueError:
                    self._buff += self._dec.decompress(stream.read(1024)).decode('utf-8')
                # EOF
                if not self._buff:
                    break

        return reader(self._obj_body)


def _download_manifest_file(obj):
    # type: (dict) -> dict
    file_content = obj['Body'].read()
    json_content = json.loads(file_content)
    return json_content


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
    # type: () -> dict
    return {
        "lineItem_UsageAmount": (_parse_float, float),
        "lineItem_BlendedRate": (_parse_float, float),
        "lineItem_BlendedCost": (_parse_float, float),
        "lineItem_UnblendedRate": (_parse_float, float),
        "lineItem_UnblendedCost": (_parse_float, float),
        "pricing_publicOnDemandCost": (_parse_float, float),
        "pricing_publicOnDemandRate": (_parse_float, float),
        "product_vcpu": (_parse_int, int),
        "product_ecu": (_parse_float, float),
        "reservation_AmortizedUpfrontCostForUsage": (_parse_float, float),
        "reservation_RecurringFeeForUsage": (_parse_float, float),
        "reservation_EffectiveCost": (_parse_float, float),
    }


def _parse_file(headers, line, event_time):
    # type: (list[str], list[str], str) -> dict
    row = {
        '@timestamp': event_time,
        'uuid': "billing_report_{}".format(event_time),
    }

    fields_parser = get_fields_parser()
    for header, tab in zip(headers, line):
        if tab:
            try:
                row[header] = fields_parser[header][0](tab)
            except KeyError:
                row[header] = tab

    return row


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

        json_content = _download_manifest_file(obj)
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

        json_content = _download_manifest_file(obj)
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

    if 'aws_access_key_id' in event and 'aws_secret_access_key' in event:
        s3client = boto3.client(
            's3',
            aws_access_key_id=event['aws_access_key_id'],
            aws_secret_access_key=event['aws_secret_access_key']
        )
    try:
        latest_csv_keys = _latest_csv_keys(s3client, env_var, event_time)
    except s3client.exceptions.NoSuchKey:
        logger.error("Could not find latest report that is in the Manifest file")
        raise

    shipper = LogzioShipper(logzio_url)
    for key in latest_csv_keys:
        logger.info("parsing the following report: {}".format(key))
        csv_like_obj = s3client.get_object(Bucket=env_var['bucket'], Key=key)
        gen = CSVLineGenerator(csv_like_obj['Body'])
        headers = next(csv.reader([gen.headers]))
        for line in gen.stream_line():
            list_line = csv.reader([line])
            shipper.add(_parse_file(headers, next(list_line), event_time))

        shipper.flush()
