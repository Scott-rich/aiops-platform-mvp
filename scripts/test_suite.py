"""Run the real unittest suite and emit counts without parsing human console output."""
import argparse
import json
import platform
from pathlib import Path
import sys
import time
import unittest


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    started=time.monotonic()
    suite=unittest.defaultTestLoader.discover('tests')
    with (args.output/'unittest.log').open('w') as stream:
        result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
    value={'status':'passed' if result.wasSuccessful() else 'failed','tests_run':result.testsRun,
        'passed':result.testsRun-len(result.failures)-len(result.errors)-len(result.skipped)-len(result.expectedFailures)-len(result.unexpectedSuccesses),
        'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
        'expected_failures':len(result.expectedFailures),'unexpected_successes':len(result.unexpectedSuccesses),
        'skip_details':[{'test':str(t),'reason':reason} for t,reason in result.skipped],
        'elapsed_seconds':time.monotonic()-started,'python':sys.version,'platform':platform.platform()}
    (args.output/'result.json').write_text(json.dumps(value,indent=2)+'\n')
    print(json.dumps(value,indent=2));return 0 if result.wasSuccessful() else 1

if __name__=='__main__':sys.exit(main())
