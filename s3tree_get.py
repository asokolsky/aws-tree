'''
Versions bucket synchronous read
'''
from argparse import ArgumentParser
import json
import sys
import time
from typing import Any, Dict, List
import boto3
import botocore.exceptions

JSON = Dict[str, Any]

verbose = False


def get_bucket_file(client, bucket: str, key: str) -> str:
    res = client.get_object(Bucket=bucket, Key=key)
    str_res = res['Body'].read().decode()
    # print('str_res', str_res)
    if verbose:
        print('Got', key, ':', str_res.strip(), file=sys.stderr)
    return str_res


def list_bucket_files(client, bucket: str) -> List[str]:
    '''
    Returns a list of all the files in the bucket
    '''
    # print(f'list_bucket_files({bucket})')
    files: List[str] = []
    paginator = client.get_paginator('list_objects')
    for page in paginator.paginate(Bucket=bucket):
        try:
            files += [p['Key'] for p in page['Contents']]
        except KeyError:
            pass

    # print(f'list_bucket_files({bucket}) => {len(files)}:{files}')
    if verbose:
        print(f'Listed {len(files)} files', file=sys.stderr)
    return files


def dict_set(d: Dict[str, Any], dirs: List[str], key: str, val: Any) -> None:
    '''
    Ensure that dirs exist in d and then set k:v there
    '''
    for dir in dirs:
        if dir not in d:
            d[dir] = {}
        d = d[dir]
    d[key] = val
    return


def get_bucket_contents(client, bucket: str) -> JSON:
    '''
    Retrieve bucket contents as JSON
    '''
    # print(f'get_bucket_contents({bucket}, {prefix})')
    res: JSON = {}
    files = list_bucket_files(client, bucket)
    if verbose:
        print(f'Listed {len(files)} files', file=sys.stderr)
    for file in files:
        if file.endswith('/'):
            continue
        content = get_bucket_file(client, bucket, file)
        content = content.strip()
        path = file.split('/')
        dict_set(res, path[:-1], path[-1], content)
    return res


def main() -> int:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='s3-tree-get', description='AWS S3 data download')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument('bucket', help='AWS S3 bucket name')
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose
    try:
        start = time.time()
        client = boto3.client('s3')
        # print(client)
        contents: JSON = get_bucket_contents(client, args.bucket)
        print(json.dumps(contents, indent=2))
        if verbose:
            print(f'Time: {time.time()-start:.2f}s', file=sys.stderr)
        return 0

    except botocore.exceptions.ClientError as err:
        print('Caught: ', err, file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    return 1


if __name__ == '__main__':
    sys.exit(main())

#
# time python s3tree_get.py clari-image-versions-test
# python s3tree_get.py clari-image-versions-test  0.75s user 0.14s system 8% cpu 11.058 total
#
