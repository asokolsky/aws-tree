'''
Versions DynamoDB synchronous read
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


def get_table_contents(client, table: str) -> List[Dict[str, Any]]:
    '''
    Retrieve table contents as Dictionary
    '''
    # print(f'get_table_contents({table})')
    results = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = client.scan(
                TableName=table, ExclusiveStartKey=last_evaluated_key)
        else:
            response = client.scan(TableName=table)
        last_evaluated_key = response.get('LastEvaluatedKey')
        results.extend(response['Items'])
        if not last_evaluated_key:
            break
    return results


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


def main() -> int:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='dynamo-tree-get', description='AWS DynamoDB data download')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument('table', help='AWS DynamoDB table name')
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose
    try:
        start = time.time()
        client = boto3.client('dynamodb')
        # print(client)
        contents: List[Dict[str, Any]] = get_table_contents(client, args.table)
        res: JSON = {}
        for record in contents:
            image = record['image']['S']
            version = record['version']['S']
            path = image.split('/')
            dict_set(res, path[:-1], path[-1], version)

        print(json.dumps(res, indent=2))
        if verbose:
            print(f'Time: {time.time()-start:.2f}s', file=sys.stderr)

    except botocore.exceptions.ClientError as err:
        print('Caught: ', err, file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    return 1


if __name__ == '__main__':
    sys.exit(main())
