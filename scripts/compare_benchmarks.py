"""Compare completed benchmark JSON without silently mixing protocols or hardware."""
import argparse
import json
from pathlib import Path
from benchmarks.report import compare_runs


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before',type=Path);parser.add_argument('after',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    value=compare_runs(json.loads(args.before.read_text()),json.loads(args.after.read_text()))
    with args.output.open('x') as stream:json.dump(value,stream,ensure_ascii=False,indent=2)
    print(json.dumps({'comparable':value['comparable'],'output':str(args.output)}))
    return 0 if value['comparable'] else 2


if __name__=='__main__':raise SystemExit(main())
