"""
hcademo

Stages simulated data in S3 to demonstrate how downstream analysis machinery
can work.

"""
import pandas as pd
import numpy as np
import json

import boto3
import argparse
import hashlib

# My buckets
# filter(lambda x: x['Name'].find('davidcs') != -1, boto3.client('s3').list_buckets()['Buckets'])

# Deleting my buckets
def my_buckets():
    return filter(lambda x: x['Name'].find('davidcs') != -1, boto3.client('s3').list_buckets()['Buckets'])

def delete_all():
    for bucket in [x['Name'] for x in my_buckets()]:
        try:
            print(bucket)
            boto3.resource('s3').Bucket(bucket).objects.delete()
            boto3.resource('s3').Bucket(bucket).delete()
        except Exception as e:
            print(e)


def simulated_feature_set(count):
    """
    Generates features meant to simulate ENSEMBL identifiers. `ENSG0000123`

    :param count:
    :return:
    """
    return ["ENSG{}".format(str(x).zfill(8)) for x in xrange(count)]


def simulated_cohort(count, start=0):
    """
    Generates samples given the count following a predictable pattern.

    :param count:
    :param start:   The sample_id to start with.
    :return:
    """
    return ["SMPL{}".format(str(x).zfill(8)) for x in xrange(
        start, count + start)]


def simulated_row_values(width):
    """
    Generates a row meant to simulate floating point RNA sequencing data.

    :param width:
    :return:
    """
    # A gaussian around 4 with std 10, clipped at 0. This is meant to capture
    # some of the sparsity of single cell data.
    return np.random.normal(4, 10, width).clip(0)


def simulated_matrix(sample_ids, feature_ids):
    """
    Returns a simulated matrix given a list of sample_ids and feature_ids.

    :param sample_ids:
    :param feature_ids:
    :return:
    """
    return pd.DataFrame(
        [simulated_row_values(len(feature_ids)) for x in sample_ids],
        index=sample_ids,
        columns=feature_ids)


def random_matrix(sample_count, feature_count, start=0):
    """
    Creates a simulated dataframe with the requested width and height by
    generating random samples.

    :param sample_count:
    :param feature_count:
    :param start: The sampled_id to start with.
    :return:
    """
    return simulated_matrix(simulated_cohort(sample_count, start=start),
                            simulated_feature_set(feature_count))


def random_tsv_matrix(sample_count, feature_count, start=0):
    """
    Creates a random TSV with the requested width and height.

    :param sample_count:
    :param feature_count:
    :param start: The sample_id to start with.
    :return:
    """
    return random_matrix(
        sample_count,
        feature_count,
        start=start).to_csv(sep='\t')


def hca_demo(datasets_count, samples_count, features_count):
    """
    Loads a simulated gene-cell matrix TSV in buckets. There is a bucket
    created for every dataset.

    Also stages an application bucket that will be used by the microservice
    to find samples. This is for demonstration only.
    :param datasets_count:
    :param samples_count:
    :param features_count:
    :return:
    """
    s3 = boto3.client('s3')
    buckets = []
    # For the demo we are going to create an index so that we know where
    # every sample is. In the future this would be done by a search engine.
    #
    # This index will be used by the microservice to "look up" which samples
    # exist in which buckets.
    application_bucket_name = "davidcs-hca-release-service"
    print(s3.create_bucket(Bucket=application_bucket_name,
                           CreateBucketConfiguration={
                               'LocationConstraint': 'us-west-2'}))
    # This will store the locations of each sample for the index.
    sample_index = {}
    for i in xrange(datasets_count):
        # generate some data to upload
        first_sample_id = i * samples_count
        tsv = random_tsv_matrix(
            samples_count,
            features_count,
            start=first_sample_id)
        # and use the digest to identify it
        digest = hashlib.sha512(tsv).hexdigest()
        print(tsv)
        bucket_name = 'davidcs-{}'.format(digest[0:20])
        # create an s3 bucket
        print(s3.create_bucket(Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}))
        # write it to a file to upload
        with open(bucket_name, 'w') as f:
            f.write(tsv)
        # then upload the file with the same name as the bucket
        print(s3.upload_file(bucket_name, bucket_name, bucket_name))
        # Add a key to the sample_index for this bucket
        sample_index[bucket_name] = simulated_cohort(
            samples_count, first_sample_id)
        # to download this file
        # s3.Bucket(bucket_name).download_file(bucket_name, bucket_name)
        buckets.append(bucket_name)
    # I know there is a better way to do this (s3 json one-liner plz).
    # Write our demonstration index.json that can be used to find samples to
    # the filesystem.
    index_filename = 'index.json'
    with open(index_filename, 'w') as f:
        f.write(json.dumps(sample_index))
    # Upload the index.json to the application bucket.
    print(s3.upload_file(
        index_filename, application_bucket_name, index_filename))
    # return the list of buckets
    return buckets


def main(args=None):
    """
    The console script that coordinates creating the demo environment.

    :param args:
    :return:
    """
    parser = argparse.ArgumentParser(
        description='Generate gene cell matrices in S3 for demonstration.')
    parser.add_argument("datasets_count", type=int,
                        help="The number of datasets to create.")
    parser.add_argument("samples_count", type=int,
                        help="The number of samples per dataset.")
    parser.add_argument("features_count", type=int,
                        help="The number of features.")

    parsed = parser.parse_args(args)
    print(parsed)
    print(hca_demo(
        parsed.datasets_count, parsed.samples_count, parsed.features_count))
