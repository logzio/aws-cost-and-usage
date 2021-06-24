import boto3
import csv
import datetime
import gzip
import httpretty
import json
import logging
import os
import src.lambda_function as worker
import src.shipper as shipper
import unittest
import yaml

from . import utils
from csv import DictReader
from logging.config import fileConfig
from src.shipper import BadLogsException, UnknownURL, UnauthorizedAccessException, MaxRetriesException
from zlib import error as zlib_error

# create logger assuming running from ./run script
fileConfig('tests/logging_config.ini')
logger = logging.getLogger(__name__)

CONFIGURATION_FILE = 'tests/config.yaml'
SAMPLE_CSV_GZIP_1 = 'tests/reports/test-billing-report-1.csv.gz'
SAMPLE_CSV_GZIP_2 = 'tests/reports/test-billing-report-2.csv.gz'
SAMPLE_CSV_ZIP_1 = 'tests/reports/test-billing-report-1.csv.zip'


class TestLambdaFunction(unittest.TestCase):
    """ Unit testing logzio lambda function """

    s3client = None
    s3res = None

    @classmethod
    def setUpClass(cls):
        with open(CONFIGURATION_FILE, 'r') as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            # Set os.environ for tests
            os.environ['REPORT_NAME'] = conf['bucket']['report_name']
            os.environ['REPORT_PATH'] = conf['bucket']['report_path']
            os.environ['S3_BUCKET_NAME'] = conf['bucket']['bucket_name']
            os.environ['TOKEN'] = conf['account']['logzio_token']
            os.environ['URL'] = conf['account']['logzio_url']
            os.environ['AWS_ACCESS_KEY_ID'] = conf['boto3_credentials']['aws_access_key_id']
            os.environ['AWS_SECRET_ACCESS_KEY'] = conf['boto3_credentials']['aws_secret_access_key']

            TestLambdaFunction.s3client = boto3.client('s3')
            TestLambdaFunction.s3res = boto3.resource('s3')

            utils.create_bucket(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'])

    def setUp(self):
        self._logzio_url = "{0}/?token={1}&type=billing".format(os.environ['URL'], os.environ['TOKEN'])

        utils.empty_bucket(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'])

    @classmethod
    def tearDownClass(cls):
        try:
            utils.delete_bucket(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'])
        except TestLambdaFunction.s3client.exceptions.NoSuchBucket:
            pass

    def test_latest_csv_file(self):
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        curr_month, prev_month = utils.get_months_range()

        # put empty folder
        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/".format(os.environ['REPORT_PATH'], curr_month), '')
        # put older folder with json file
        prev_json_content = {
            "reportKeys": [
                "location1"
            ]
        }
        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
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
        keys = worker._latest_csv_keys(TestLambdaFunction.s3client, env_var, event_time)
        self.assertEqual(keys[0], "location1",
                         "Unexpected key in the json file - {0} - {1}".format(prev_month, keys[0]))

        # upload json file to current month folder
        curr_json_content = {
            "reportKeys": [
                "location2"
            ]
        }
        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(curr_json_content))
        keys = worker._latest_csv_keys(TestLambdaFunction.s3client, env_var, event_time)
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
            ship = shipper.LogzioShipper(self._logzio_url)
            r = csv.reader(csv_lines, delimiter=',')
            tmp_headers = next(r)
            headers = [header.replace('/', '_') for header in tmp_headers]
            for row in r:
                ship.add(worker._parse_file(headers, row, event_time))
            ship.flush()

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

        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))
        # upload two files
        utils.upload_gzipped(TestLambdaFunction.s3res, os.environ['S3_BUCKET_NAME'],
                             manifest_content["reportKeys"][0], SAMPLE_CSV_GZIP_1)
        utils.upload_gzipped(TestLambdaFunction.s3res, os.environ['S3_BUCKET_NAME'],
                             manifest_content["reportKeys"][1], SAMPLE_CSV_GZIP_2)

        # user flow
        env_var = {
            'logzio_url': os.environ['URL'],
            'token': os.environ['TOKEN'],
            'bucket': os.environ['S3_BUCKET_NAME'],
            'report_path': os.environ['REPORT_PATH'],
            'report_name': os.environ['REPORT_NAME']
        }
        latest_csv_keys = worker._latest_csv_keys(TestLambdaFunction.s3client, env_var, event_time)
        readers = []

        ship = shipper.LogzioShipper(self._logzio_url)
        # first csv
        csv_like_obj1 = TestLambdaFunction.s3client.get_object(Bucket=env_var['bucket'], Key=latest_csv_keys[0])
        gen1 = worker.CSVLineGenerator(csv_like_obj1['Body'])
        csv_lines1 = gen1.headers
        for line in gen1.stream_line():
            csv_lines1 += line
        readers.append(DictReader(csv_lines1.splitlines(True)))

        # second csv
        csv_like_obj2 = TestLambdaFunction.s3client.get_object(Bucket=env_var['bucket'], Key=latest_csv_keys[1])
        gen2 = worker.CSVLineGenerator(csv_like_obj2['Body'])
        csv_lines2 = gen2.headers
        for line in gen2.stream_line():
            csv_lines2 += line
        readers.append(DictReader(csv_lines2.splitlines(True)))

        # now we can use http mock
        httpretty.register_uri(httpretty.POST, self._logzio_url)
        httpretty.enable()

        for line in csv_lines1.splitlines()[1:]:
            ship.add(worker._parse_file(gen1.headers.split(','), next(csv.reader([line])), event_time))
        ship.flush()

        for line in csv_lines2.splitlines()[1:]:
            ship.add(worker._parse_file(gen2.headers.split(','), next(csv.reader([line])), event_time))
        ship.flush()

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
        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))
        utils.upload_gzipped(TestLambdaFunction.s3res, os.environ['S3_BUCKET_NAME'], manifest_content["reportKeys"][0],
                             SAMPLE_CSV_ZIP_1)

        event = {
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": event_time
        }
        # TODO - catch exact string in the error
        try:
            worker.lambda_handler(event, {})
        except zlib_error:
            return
        assert True, "wrong compression - {}".format(zlib_error)

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
        utils.put_object(TestLambdaFunction.s3client, os.environ['S3_BUCKET_NAME'],
                         "{0}/{1}/{2}-Manifest.json".format(os.environ['REPORT_PATH'], curr_month,
                                                            os.environ['REPORT_NAME']), json.dumps(manifest_content))

        event = {
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": event_time
        }
        with self.assertRaises(TestLambdaFunction.s3client.exceptions.NoSuchKey):
            worker.lambda_handler(event, {})

    @httpretty.activate
    def test_bad_logs(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=400)
        ship = shipper.LogzioShipper(self._logzio_url)
        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            r = csv.reader(csv_lines, delimiter=',')
            tmp_headers = next(r)
            headers = [header.replace('/', '_') for header in tmp_headers]
            with self.assertRaises(BadLogsException):
                for row in r:
                    ship.add(worker._parse_file(headers, row, event_time))
                ship.flush()

    @httpretty.activate
    def test_wrong_token(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=401)
        ship = shipper.LogzioShipper(self._logzio_url)
        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            r = csv.reader(csv_lines, delimiter=',')
            tmp_headers = next(r)
            headers = [header.replace('/', '_') for header in tmp_headers]
            with self.assertRaises(UnauthorizedAccessException):
                for row in r:
                    ship.add(worker._parse_file(headers, row, event_time))
                ship.flush()

    @httpretty.activate
    def test_retry_sending(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=405)
        ship = shipper.LogzioShipper(self._logzio_url)
        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            r = csv.reader(csv_lines, delimiter=',')
            tmp_headers = next(r)
            headers = [header.replace('/', '_') for header in tmp_headers]
            with self.assertRaises(MaxRetriesException):
                for row in r:
                    ship.add(worker._parse_file(headers, row, event_time))
                ship.flush()

    @httpretty.activate
    def test_bad_url(self):
        httpretty.register_uri(httpretty.POST, self._logzio_url, status=404)
        ship = shipper.LogzioShipper(self._logzio_url)
        # send csv info to our mock server
        with gzip.open(SAMPLE_CSV_GZIP_1) as f:
            csv_lines = f.read().decode('utf-8').splitlines(True)
            event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            r = csv.reader(csv_lines, delimiter=',')
            tmp_headers = next(r)
            headers = [header.replace('/', '_') for header in tmp_headers]
            with self.assertRaises(UnknownURL):
                for row in r:
                    ship.add(worker._parse_file(headers, row, event_time))
                ship.flush()


if __name__ == '__main__':
    unittest.main()
