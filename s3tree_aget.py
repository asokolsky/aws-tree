'''
Versions bucket read asynchronously

Relies on https://github.com/aio-libs/aiobotocore
Install it using:
pip install -U 'aiobotocore[awscli,boto3]'
pip install types-aiobotocore
'''
from argparse import ArgumentParser
import asyncio
import json
import sys
import time
from typing import Any, Dict, List

from aiobotocore.session import get_session

JSON = Dict[str, Any]

verbose = False


async def get_bucket_file(client, bucket: str, key: str) -> str:
    res = await client.get_object(Bucket=bucket, Key=key)
    bin_res = await res['Body'].read()
    str_res = bin_res.decode()
    # print('str_res', str_res)
    if verbose:
        print('Got', key, ':', str_res.strip(), file=sys.stderr)
    return str_res


async def list_bucket_files(client, bucket: str) -> List[str]:
    '''
    Returns a list of all the files in the bucket
    '''
    # print(f'list_bucket_files({bucket})')
    files: List[str] = []
    paginator = client.get_paginator('list_objects')
    async for page in paginator.paginate(Bucket=bucket):
        try:
            files += [p['Key'] for p in page['Contents']]
        except KeyError:
            pass
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


async def get_bucket_contents(client, bucket: str) -> JSON:
    '''
    Retrieve bucket contents as JSON
    '''
    # print(f'get_bucket_contents({bucket}, {prefix})')
    files = await list_bucket_files(client, bucket)
    tasks = [
        asyncio.create_task(get_bucket_file(client, bucket, f)) for f in files
    ]
    contents = await asyncio.gather(*tasks)
    res: JSON = {}
    for file, content in zip(files, contents):
        path = file.split('/')
        dict_set(res, path[:-1], path[-1], content.strip())
    return res


async def main() -> None:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='s3-tree-aget', description='AWS S3 data download')
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
        session = get_session()
        async with session.create_client('s3') as client:
            # print(client)
            contents: JSON = await get_bucket_contents(client, args.bucket)
            print(json.dumps(contents, indent=2))
            if verbose:
                print(f'Time: {time.time()-start:.2f}s', file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    return


if __name__ == '__main__':
    asyncio.run(main())

#
# time python s3tree_aget.py clari-image-versions-test
# python s3tree_aget.py clari-image-versions-test  0.64s user 0.15s system 34% cpu 2.333 total
#
