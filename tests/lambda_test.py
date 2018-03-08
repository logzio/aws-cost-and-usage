import os
import boto3
import datetime
import gzip
import httpretty
import json
import logging
import src.lambda_function as worker
import unittest
import utils
import yaml

from csv import DictReader
from logging.config import fileConfig

# create logger assuming running from ./run script
fileConfig('tests/logging_config.ini')
logger = logging.getLogger(__name__)

CONFIGURATION_FILE = 'tests/config.yaml'
SAMPLE_CSV_GZIP_1 = 'tests/reports/test-billing-report-1.csv.gz'
SAMPLE_CSV_GZIP_2 = 'tests/reports/test-billing-report-2.csv.gz'
SAMPLE_CSV_ZIP_1 = 'tests/reports/test-billing-report-1.csv.zip'

s3client = boto3.client('s3')


class TestLambdaFunction(unittest.TestCase):
    """ Unit testing logzio lambda function """

    def setUp(self):
        with open(CONFIGURATION_FILE, 'r') as f:
            conf = yaml.load(f)
            # Set os.environ for tests
            os.environ['REPORT_NAME'] = conf['bucket']['report_name']
            os.environ['REPORT_PATH'] = conf['bucket']['report_path']
            os.environ['S3_BUCKET_NAME'] = conf['bucket']['bucket_name']
            os.environ['TOKEN'] = conf['account']['logzio_token']
            os.environ['URL'] = conf['account']['logzio_url']

        self._logzio_url = "{0}/?token={1}&type=billing".format(os.environ['URL'], os.environ['TOKEN'])

        utils.create_bucket(s3client, os.environ['S3_BUCKET_NAME'])
        self._bucket_policy = {
            "Version": "2008-10-17",
            "Id": "Policy1335892530063",
            "Statement": [
                {
                    "Sid": "Stmt1335892150622",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::386209384616:root"
                    },
                    "Action": [
                        "s3:GetBucketAcl",
                        "s3:GetBucketPolicy"
                    ],
                    "Resource": "arn:aws:s3:::{}".format(os.environ['S3_BUCKET_NAME'])
                },
                {
                    "Sid": "Stmt1335892526596",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::386209384616:root"
                    },
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject"
                    ],
                    "Resource": "arn:aws:s3:::{}/*".format(os.environ['S3_BUCKET_NAME'])
                }
            ]
        }
        utils.put_bucket_policy(s3client, os.environ['S3_BUCKET_NAME'], json.dumps(self._bucket_policy))

    def tearDown(self):
        try:
            utils.delete_bucket(s3client, os.environ['S3_BUCKET_NAME'])
        except s3client.exceptions.NoSuchBucket:
            pass

    def test_latest_csv_file(self):
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        curr_month, prev_month = utils.get_months_range()

        # put empty folder
        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/".format(os.environ['REPORT_PATH'], curr_month), '')
        # put older folder with json file
        prev_json_content = {
            "reportKeys": [
                "location1"
            ]
        }
        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], prev_month,
                                                            os.environ['REPORT_NAME']), json.dumps(prev_json_content))

        # check we find it in the previous month
        env_var = {
            'logzio_url': os.environ['URL'],
            'token': os.environ['TOKEN'],
            'bucket': os.environ['S3_BUCKET_NAME'],
            'report_path': os.environ['REPORT_PATH'],
            'report_name': os.environ['REPORT_NAME']
        }
        keys = worker._latest_csv_keys(s3client, env_var, event_time)
        self.assertEqual(keys[0], "location1",
                         "Unexpected key in the json file - {0} - {1}".format(prev_month, keys[0]))

        # upload json file to current month folder
        curr_json_content = {
            "reportKeys": [
                "location2"
            ]
        }
        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(curr_json_content))
        keys = worker._latest_csv_keys(s3client, env_var, event_time)
        self.assertEqual(keys[0], "location2",
                         "Unexpected key in the json file - {0} - {1}".format(curr_month, keys[0]))

    def test_missing_environment_variable(self):
        env_vars = ['URL', 'TOKEN', 'S3_BUCKET_NAME', 'REPORT_PATH', 'REPORT_NAME']
        for v in env_vars:
            tmp = os.environ[v]
            del os.environ[v]
            self.assertRaises(KeyError, worker.lambda_handler, None, None)
            os.environ[v] = tmp

    @httpretty.activate
    def test_parsed_logs(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url)

        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            worker._parse_file(csv_lines, self._logzio_url, event_time)

            reader = DictReader(csv_lines)
            self.assertTrue(utils.verify_requests([reader], httpretty.HTTPretty.latest_requests),
                            "Something wrong parsing...")

    # mock server will block boto3 calls - enable it only when needed
    def test_multi_reports(self):
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        curr_month, prev_month = utils.get_months_range()
        key = "{0}/{1}/12345678-1234-1234-1234-123456789123/{2}".format(os.environ['REPORT_PATH'], curr_month,
                                                                        os.environ['REPORT_NAME'])
        # set two files as latest report
        manifest_content = {
            "reportKeys": [
                "{}-1.csv.gz".format(key),
                "{}-2.csv.gz".format(key)
            ]
        }

        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))
        # upload two files
        s3res = boto3.resource('s3')
        utils.upload_gzipped(s3res, os.environ['S3_BUCKET_NAME'], manifest_content["reportKeys"][0], SAMPLE_CSV_GZIP_1)
        utils.upload_gzipped(s3res, os.environ['S3_BUCKET_NAME'], manifest_content["reportKeys"][1], SAMPLE_CSV_GZIP_2)

        # user flow
        env_var = {
            'logzio_url': os.environ['URL'],
            'token': os.environ['TOKEN'],
            'bucket': os.environ['S3_BUCKET_NAME'],
            'report_path': os.environ['REPORT_PATH'],
            'report_name': os.environ['REPORT_NAME']
        }
        latest_csv_keys = worker._latest_csv_keys(s3client, env_var, event_time)
        readers = []

        # first csv
        csv_like_obj1 = s3client.get_object(Bucket=env_var['bucket'], Key=latest_csv_keys[0])
        csv_obj1 = worker._download_object(csv_like_obj1, True)
        csv_lines1 = csv_obj1.splitlines(True)
        readers.append(DictReader(csv_lines1))

        # second csv
        csv_like_obj2 = s3client.get_object(Bucket=env_var['bucket'], Key=latest_csv_keys[1])
        csv_obj2 = worker._download_object(csv_like_obj2, True)
        csv_lines2 = csv_obj2.splitlines(True)
        readers.append(DictReader(csv_lines2))

        # now we can use http mock
        httpretty.register_uri(httpretty.POST, self._logzio_url)
        httpretty.enable()
        worker._parse_file(csv_lines1, self._logzio_url, event_time)
        worker._parse_file(csv_lines2, self._logzio_url, event_time)

        self.assertTrue(utils.verify_requests(readers, httpretty.HTTPretty.latest_requests),
                        "Something wrong parsing...")

        httpretty.disable()

    def test_wrong_compression_format(self):
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        curr_month, prev_month = utils.get_months_range()
        key = "{0}/{1}/12345678-1234-1234-1234-123456789123/{2}".format(os.environ['REPORT_PATH'], curr_month,
                                                                        os.environ['REPORT_NAME'])
        manifest_content = {
            "reportKeys": [
                "{}-1.csv.zip".format(key)
            ]
        }
        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))
        s3res = boto3.resource('s3')
        utils.upload_gzipped(s3res, os.environ['S3_BUCKET_NAME'], manifest_content["reportKeys"][0], SAMPLE_CSV_ZIP_1)

        event = {
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": event_time
        }
        with self.assertRaises(IOError):
            worker.lambda_handler(event, None)

    def test_no_report(self):
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        curr_month, prev_month = utils.get_months_range()
        key = "{0}/{1}/12345678-1234-1234-1234-123456789123/{2}".format(os.environ['REPORT_PATH'], curr_month,
                                                                        os.environ['REPORT_NAME'])
        manifest_content = {
            "reportKeys": [
                "{}-1.csv.gz".format(key)
            ]
        }
        utils.put_object(s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))

        event = {
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": event_time
        }
        with self.assertRaises(s3client.exceptions.NoSuchKey):
            worker.lambda_handler(event, None)

    @httpretty.activate
    def test_bad_logs(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=400)

        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with self.assertRaises(worker.BadLogsException):
                worker._parse_file(csv_lines, self._logzio_url, event_time)

    @httpretty.activate
    def test_wrong_token(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=401)

        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with self.assertRaises(worker.UnauthorizedAccessException):
                worker._parse_file(csv_lines, self._logzio_url, event_time)

    @httpretty.activate
    def test_retry_sending(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=500)

        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with self.assertRaises(worker.MaxRetriesException):
                worker._parse_file(csv_lines, self._logzio_url, event_time)


if __name__ == '__main__':
    unittest.main()
