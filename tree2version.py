from argparse import ArgumentParser, FileType
import json
import sys
from typing import Any, Dict, List


def tree2versions(path: List[str], tree: Dict[str, Any],
                  versions: Dict[str, Any]) -> None:
    for k,v in tree.items():
        if isinstance(v, dict):
            tree2versions(path + [k], v, versions)
        else:
            path_str = '/'.join(path)
            if path_str not in versions:
                versions[path_str] = {}
            versions[path_str][k] = v
    return


def main() -> int:
    '''
    Main entry point
    '''
    ap = ArgumentParser(
        prog='tree2version',
        description='Convert version tree to versions.json')
    ap.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='make helpful noises')
    ap.add_argument(
        'input', nargs='?', type=FileType('r'), default=sys.stdin)
    #
    # parse the command line
    #
    args = ap.parse_args()
    global verbose
    verbose = args.verbose
    try:
        tree = json.load(args.input)
        versions: Dict[str, Any] = {}
        tree2versions([], tree, versions)
        print(json.dumps(versions, indent=2))

    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt', file=sys.stderr)

    return 1


if __name__ == '__main__':
    sys.exit(main())

#
#  python s3tree_get.py clari-image-versions-test  >tree.json
#  cat tree.json|python tree2version.py
#
