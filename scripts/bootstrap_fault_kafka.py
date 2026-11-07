"""Download checksum-verified Kafka only; no services are started."""
import argparse
import json
from pathlib import Path
import tarfile
from scripts.bootstrap_native import KAFKA_ARCHIVE, KAFKA_VERSION, fetch, verify_published_sha512


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir',type=Path,required=True)
    args=parser.parse_args()
    root=args.runtime_dir.resolve();root.mkdir(parents=True,exist_ok=True)
    url=f'https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}'
    fetch(url,root/KAFKA_ARCHIVE);fetch(url+'.sha512',root/(KAFKA_ARCHIVE+'.sha512'))
    sha=verify_published_sha512(root/KAFKA_ARCHIVE,root/(KAFKA_ARCHIVE+'.sha512'))
    home=root/f'kafka_2.13-{KAFKA_VERSION}'
    if not home.exists():
        with tarfile.open(root/KAFKA_ARCHIVE) as archive:
            for member in archive.getmembers():
                if member.issym() or member.islnk() or not (root/member.name).resolve().is_relative_to(root):
                    raise ValueError('unsafe Kafka archive member')
            archive.extractall(root,filter='data')
    result={'kafka_home':str(home),'version':KAFKA_VERSION,'sha512':sha}
    (root/'kafka-runtime.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))


if __name__=='__main__':main()
