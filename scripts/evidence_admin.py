"""Local-only evidence archive/retention CLI; pruning defaults to dry-run."""
import argparse
import json
import logging
import sys
from app.storage import Store
from app.evidence_archive import export_set, verify_archive, restore_events, prune_events


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', default='data')
    sub = parser.add_subparsers(dest='command', required=True)
    archive = sub.add_parser('archive'); archive.add_argument('--set-id', required=True); archive.add_argument('--output', required=True)
    verify = sub.add_parser('verify-archive'); verify.add_argument('--input', required=True)
    restore = sub.add_parser('restore'); restore.add_argument('--input', required=True)
    prune = sub.add_parser('prune'); prune.add_argument('--before', type=int, required=True)
    prune.add_argument('--output-dir', default='archives'); prune.add_argument('--batch-size', type=int, default=500)
    prune.add_argument('--apply', action='store_true', help='backup, archive, verify, then delete ONE bounded batch of raw events')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    try:
        if args.command == 'verify-archive':
            result = verify_archive(args.input)
        else:
            store = Store(args.data_dir)
            if args.command == 'archive':
                result = export_set(store, args.set_id, args.output)
            elif args.command == 'restore':
                result = restore_events(store, args.input)
            else:
                result = prune_events(store, args.before, args.output_dir, args.batch_size, args.apply)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        logging.getLogger(__name__).error('evidence_admin_failed error_type=%s', type(exc).__name__)
        print(json.dumps({'ok': False, 'error_type': type(exc).__name__, 'message': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
