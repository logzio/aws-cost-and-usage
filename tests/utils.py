import datetime
import dateutil.relativedelta
import json
import logging

from logging.config import fileConfig
from src.lambda_function import get_fields_parser

fileConfig('tests/logging_config.ini')
logger = logging.getLogger()


def create_bucket(s3client, bucket):
    try:
        response = s3client.create_bucket(
            Bucket=bucket
        )
    except Exception as e:
        logger.error("Unexpected error while create_bucket: {}".format(e))
        raise
    logger.info("create_bucket response: {}".format(response))


def delete_bucket(s3client, bucket):
    # before deleting bucket we need to delete all objects
    paginator = s3client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    delete_us = dict(Objects=[])

    for item in pages.search('Contents'):
        if item is None:
            continue
        delete_us['Objects'].append(dict(Key=item['Key']))

        # flush once aws limit reached
        if len(delete_us['Objects']) >= 1000:
            try:
                response = s3client.delete_objects(Bucket=bucket, Delete=delete_us)
                logger.info("delete_objects response: {}".format(response))
                delete_us = dict(Objects=[])
            except Exception as e:
                logger.error("Unexpected error while delete_objects: {}".format(e))
                raise

    # flush rest
    if len(delete_us['Objects']):
        try:
            response = s3client.delete_objects(Bucket=bucket, Delete=delete_us)
            logger.info("delete_objects response: {}".format(response))
        except Exception as e:
            logger.error("Unexpected error while delete_objects: {}".format(e))
            raise

    # delete bucket
    try:
        response = s3client.delete_bucket(Bucket=bucket)
        logger.info("delete_bucket response: {}".format(response))
    except Exception as e:
        logger.error("Unexpected error while delete_bucket: {}".format(e))
        raise


def put_bucket_policy(s3client, bucket, policy):
    # check json format
    try:
        json.loads(policy)
    except ValueError:
        logger.error("Policy should be a valid json")
        raise

    try:
        response = s3client.put_bucket_policy(
            Bucket=bucket,
            Policy=policy
        )
    except Exception as e:
        logger.error("Unexpected error while put_bucket_policy: {}".format(e))
        raise
    logger.info("put_bucket_policy response: {}".format(response))


def put_object(s3client, bucket, key, body):
    try:
        response = s3client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
        )

    except Exception as e:
        logger.error("Unexpected error while put_object: {}".format(e))
        raise
    logger.info("put_bucket_policy response: {}".format(response))


def create_manifest(report_keys, report_name):
    json_data = json.dumps(
        {
            'reportKeys': report_keys
        }
    )
    with open(report_name, 'w') as f:
        json.dump(json_data, f)


def get_months_range():
    start = datetime.datetime.now()
    end = start + dateutil.relativedelta.relativedelta(months=1)
    report_monthly_folder = "{:02d}{:02d}01-{:02d}{:02d}01".format(start.year, start.month, end.year, end.month)
    end = start
    start = end - dateutil.relativedelta.relativedelta(months=1)
    previous_report_monthly_folder = "{:02d}{:02d}01-{:02d}{:02d}01"\
        .format(start.year, start.month, end.year, end.month)
    return report_monthly_folder, previous_report_monthly_folder


def verify_requests(csv_readers, requests):
    csvs_row_count, req_row_count = 0, 0
    headers = set()
    headers.update(["@timestamp", "uuid"])
    fields_parser = get_fields_parser()
    for reader in csv_readers:
        headers.update(header.replace('/', '_') for header in reader.fieldnames)
        csvs_row_count += sum(1 for row in reader)

    for request in requests:
        for req in request.parsed_body.splitlines():
            req_row_count += 1
            tested = json.loads(req)
            for key, value in tested.iteritems():
                # check empty columns are not in the request json
                if value == '':
                    logger.error("Unexpected empty key in the request")
                    return False
                # no additional headers
                elif key not in headers:
                    logger.error("Unexpected key: {}".format(key))
                    return False
                # type is the correct one
                elif key in fields_parser:
                    if type(value) != fields_parser[key][1]:
                        logger.error("Unexpected type: {} - {} vs. {}"
                                     .format(value, type(value), fields_parser[key][1]))
                        return False
                # unicode
                else:
                    if type(value) != unicode:
                        logger.error("Unexpected type: {} - {} vs. unicode"
                                     .format(value, type(value)))
                        return False

    if csvs_row_count != req_row_count:
        logger.error("expected {} rows vs. {} rows sent".format(csvs_row_count, req_row_count))
        return False

    return True


def upload_gzipped(s3conn, bucket, key, fp):
    try:
        s3conn.Bucket(bucket).upload_file(fp, key)
    except Exception as e:
        logger.error("Unexpected error while upload_file: {}".format(e))
        raise
