from argparse import ArgumentParser, FileType
import json
import sys
import time
from typing import Any, Dict, List
import boto3
import botocore.exceptions

JSON = Dict[str, Any]

verbose = False


def main() -> int:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='versions2dynamodb',
        description='Upload versions.json to DynamoDB')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument(
        'table', help='DynamoDB table name')
    ap.add_argument(
        'input', help='path to versions.json',
        type=FileType('r', encoding='UTF-8'))
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose
    try:
        start = time.time()
        versions = json.load(args.input)
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(args.table)
        for path, vs in versions.items():
            # path is "clarius.jfrog.io/clari-docker-v0-virtual/clari/foo"
            # vs is {'dev':'hash', 'stage':'hash'}
            for env, ver in vs.items():
                item = {
                    'image': path + '/' + env,
                    'version': ver,
                }
                print(item)
                table.put_item(Item=item)

        if verbose:
            print(f'Time: {time.time()-start:.2f}s', file=sys.stderr)

    except json.JSONDecodeError as err:
        print('JSONDecodeError', err, file=sys.stderr)

    except botocore.exceptions.ClientError as err:
        print('Caught: ', err, file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    args.input.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
