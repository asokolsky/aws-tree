'''
Versions bucket write
'''
from argparse import ArgumentParser, FileType
import json
import sys
from typing import Any, Dict
import boto3
import botocore.exceptions

JSON = Dict[str, Any]

verbose = False


def put_bucket(client, bucket: str, path: str, key: str, val: str) -> bool:
    full_key = path + '/' + key
    if verbose:
        print('Set', full_key, 'to', val, file=sys.stderr)

    res = client.put_object(Body=val.encode(), Bucket=bucket, Key=full_key)
    # print(res)
    return res['ResponseMetadata']['HTTPStatusCode'] == 200


def main() -> int:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='s3-tree-put-versions', description='AWS S3 data upload')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument('bucket', help='AWS S3 bucket name')
    ap.add_argument(
        'input', help='path to versions.json',
        type=FileType('r', encoding='UTF-8'))
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose

    ec = 0
    try:
        versions = json.load(args.input)
        client = boto3.client('s3')
        for path, vs in versions.items():
            # path is "clarius.jfrog.io/clari-docker-v0-virtual/clari/foo"
            # vs is {'dev':'hash', 'stage':'hash'}
            for env, ver in vs.items():
                if put_bucket(client, args.bucket, path, env, ver):
                    continue
                print('Faild to put', args.bucket, path, env, ver,
                      file=sys.stderr)
                ec = 1
                break
            if ec:
                break

    except botocore.exceptions.ClientError as err:
        print('Caught: ', err, file=sys.stderr)
        ec = 2

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)
        ec = 3

    args.input.close()
    return ec


if __name__ == '__main__':
    sys.exit(main())

#
# python s3tree_put_versions.py clari-image-versions-test ~/Projects/k8s/lib/imported/versions.json
#
