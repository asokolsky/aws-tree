'''
Versions bucket write
'''
from argparse import ArgumentParser
import sys
from typing import Any, Dict
import boto3
import botocore.exceptions

JSON = Dict[str, Any]

verbose = False


def put_bucket(bucket: str, path: str, key: str, val: str) -> bool:
    client = boto3.client('s3')
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
    ap = ArgumentParser(prog='s3-tree-put', description='AWS S3 data upload')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument('bucket', help='AWS S3 bucket name')
    ap.add_argument('key', help='path, e.g. quay.io/wish/nodetaint')
    ap.add_argument('env', help='env, e.g. dev or stage or prod')
    ap.add_argument('version', help='hash, e.g. latest')
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose
    try:
        if put_bucket(args.bucket, args.key, args.env, args.version):
            return 0

    except botocore.exceptions.ClientError as err:
        print('Caught: ', err, file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    return 1


if __name__ == '__main__':
    sys.exit(main())


#
# python s3tree_put.py clari-image-versions-test quay.io/wish/nodetaint shared v0.3.2
# python s3tree_put.py clari-image-versions-test quay.io/wish/nodetaint shared   0.34s user 0.18s system 45% cpu 1.134 total
