"""Verify additive phase7 migration on an online backup, leaving the source unchanged."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import traceback

from app.storage import Store


def digest_file(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''): value.update(chunk)
    return value.hexdigest()


def inventory(path):
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
        integrity = [r[0] for r in db.execute('PRAGMA integrity_check')]
        tables = {}
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            checksum = hashlib.sha256(); count = 0
            quoted = '"'+name.replace('"', '""')+'"'
            for row in db.execute('SELECT * FROM '+quoted+' ORDER BY rowid'):
                checksum.update((json.dumps(row, ensure_ascii=True, separators=(',', ':'), allow_nan=False)+'\n').encode())
                count += 1
            tables[name] = {'rows': count, 'payload_sha256': checksum.hexdigest()}
        return {'integrity': integrity, 'tables': tables}


def verify(source, output):
    source = source.resolve(); output = output.resolve()
    if not source.is_file(): raise FileNotFoundError(source)
    output.mkdir(parents=True, exist_ok=False)
    data = output/'data'; data.mkdir()
    target = data/'aiops.sqlite3'
    result = {'started_at_utc': datetime.now(timezone.utc).isoformat(), 'source': str(source),
              'execution_kind': 'real_sqlite_backup_and_migration', 'status': 'running', 'checks': []}
    def check(name, condition):
        result['checks'].append({'name': name, 'passed': bool(condition)})
        if not condition: raise AssertionError(name)
    try:
        original_hash = digest_file(source)
        deadline = time.monotonic()+10
        def progress(*_):
            if time.monotonic() > deadline: raise TimeoutError('source backup exceeded 10 seconds')
        with closing(sqlite3.connect(source.as_uri()+'?mode=ro', uri=True)) as src:
            with closing(sqlite3.connect(target)) as dst:
                src.backup(dst, pages=256, progress=progress, sleep=.05)
        before = inventory(target); result['before'] = before
        check('source_snapshot_integrity_ok', before['integrity'] == ['ok'])
        check('source_is_pre_phase7', 'pipeline_fast_state' not in before['tables'])
        store = Store(data)
        after = inventory(target); result['after'] = after
        check('all_original_tables_and_payloads_preserved', all(after['tables'].get(k) == v for k, v in before['tables'].items()))
        check('upgraded_integrity_ok', after['integrity'] == ['ok'])
        with store.db() as db:
            backup = Path(db.execute("SELECT backup_path FROM platform_migrations WHERE version='phase7-v1'").fetchone()[0])
        result['migration_backup'] = str(backup)
        check('automatic_pre_migration_backup_matches_original', inventory(backup) == before)
        check('source_file_bytes_unchanged', digest_file(source) == original_hash)
        result.update(source_sha256=original_hash, status='passed')
    except Exception as exc:
        result.update(status='failed', error={'type': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()})
    result['ended_at_utc'] = datetime.now(timezone.utc).isoformat()
    (output/'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Existing pre-phase7 aiops.sqlite3; preferably quiesced')
    parser.add_argument('--output', type=Path, required=True, help='New, isolated verification directory')
    args = parser.parse_args(); result = verify(args.source, args.output)
    print(json.dumps({'status': result['status'], 'checks': result['checks']}, ensure_ascii=False))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__': raise SystemExit(main())
