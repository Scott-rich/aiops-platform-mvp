"""Derive exact pytest counts without modifying a frozen quality result."""
from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from research.experiment import Experiment, sha256


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result_path=args.input/'result.json';log=args.input/'tests.log'
    original=json.loads(result_path.read_text())
    terminal=json.loads((args.input/'completed.json').read_text())
    assert terminal['result_sha256']==sha256(result_path)
    counts=re.findall(r'(\d+) passed, (\d+) skipped',log.read_text())
    assert len(counts)==1
    passed,skipped=map(int,counts[0])
    with Experiment(args.output,{'input_result_sha256':sha256(result_path),'input_log_sha256':sha256(log),'input':str(args.input),'helper_sha256':sha256(Path(__file__))},[2026]) as run:
        run.result['metrics']={'tests_passed':passed,'tests_skipped':skipped,'tests_failed':0 if original['metrics']['checks']['tests']['exit_code']==0 else None,'coverage':original['metrics']['coverage'],'original_status':original['status']}
        assert original['status']=='passed'
        run.summary(['从原始终态与pytest日志提取实际计数；不改写原结果。','完整质量测试输出。','不是新的测试运行。','运行计数不估计泛化CI。','仅对应输入哈希版本。','继续保持源码与结果映射。'])

if __name__=='__main__':main()
