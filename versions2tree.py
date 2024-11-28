from argparse import ArgumentParser, FileType
import json
import sys
from typing import Any, Dict, List


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
        prog='versions2tree', description='Convert versions.json to tree json')
    ap.add_argument(
        'input', help='path to versions.json',
        type=FileType('r', encoding='UTF-8'))
    #
    # parse the command line
    #
    args = ap.parse_args()
    try:
        versions = json.load(args.input)
        res: Dict[str, Any] = {}
        for path, vs in versions.items():
            # path is "clarius.jfrog.io/clari-docker-v0-virtual/clari/foo"
            # vs is {'dev':'hash', 'stage':'hash'}
            dirs = path.split('/')
            for env, ver in vs.items():
                dict_set(res, dirs, env, ver)
        print(json.dumps(res, indent=2))
    except json.JSONDecodeError as err:
        print('JSONDecodeError', err, file=sys.stderr)

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    args.input.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

#
# python versions2tree.py ~/Projects/k8s/lib/imported/versions.json
#
