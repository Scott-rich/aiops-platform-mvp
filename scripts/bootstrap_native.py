"""Install pinned, isolated Kafka/PyFlink tools for the optional Linux smoke test.

No service, system package, Docker daemon or user database is modified. Downloaded
Kafka is checked against Apache's SHA-512 before extraction. The older Maven
connector publishes SHA-1 only; that integrity check is recorded as SHA-1, with
an additional local SHA-256 fingerprint, rather than mislabelled as SHA-512.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import urllib.request

KAFKA_VERSION = "3.9.1"
FLINK_VERSION = "1.20.3"
CONNECTOR = "flink-sql-connector-kafka-3.3.0-1.20.jar"
ECJ = "ecj-3.33.0.jar"
KAFKA_ARCHIVE = f"kafka_2.13-{KAFKA_VERSION}.tgz"


def fetch(url: str, target: Path) -> None:
    if target.exists():
        return
    temporary = target.with_suffix(target.suffix + ".partial")
    try:
        with urllib.request.urlopen(url, timeout=30) as source, temporary.open("wb") as dest:
            shutil.copyfileobj(source, dest, 1024 * 1024)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def verify_published_sha512(artifact: Path, checksum: Path) -> str:
    content = checksum.read_text()
    # Apache sometimes groups SHA-512 into sixteen 8-hex blocks.
    if ":" in content:
        content = content.split(":", 1)[1]
    normalized = "".join(content.split())
    matches = re.findall(r"\b[a-fA-F0-9]{128}\b", normalized)
    if len(matches) != 1:
        raise ValueError(f"ambiguous SHA-512 file: {checksum.name}")
    actual = hashlib.sha512(artifact.read_bytes()).hexdigest()
    if actual != matches[0].lower():
        raise ValueError(f"SHA-512 mismatch: {artifact.name}")
    return actual


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=Path("/tmp/aiops-native-runtime"))
    args = parser.parse_args()
    runtime = args.runtime_dir.expanduser().resolve()
    downloads = runtime / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    if not shutil.which("java") or not shutil.which("uv"):
        raise SystemExit("Native test requires Java 17 and uv on PATH; no system installer is run.")
    subprocess.run(["uv", "python", "install", "3.11"], check=True)
    python = runtime / "venv/bin/python"
    if not python.exists():
        subprocess.run(["uv", "venv", "--python", "3.11", str(runtime / "venv")], check=True)
    subprocess.run(["uv", "pip", "install", "--python", str(python),
                    f"apache-flink=={FLINK_VERSION}", "kafka-python==2.2.12",
                    "fastapi==0.115.12", "uvicorn==0.34.2", "httpx==0.28.1", "pydantic==2.14.1", "filelock==4.2.4",
                    "setuptools==80.9.0", "prometheus-client==0.22.1"], check=True)
    kafka_url = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}"
    connector_url = ("https://repo.maven.apache.org/maven2/org/apache/flink/"
                     "flink-sql-connector-kafka/3.3.0-1.20/" + CONNECTOR)
    hashes = {}
    fetch(kafka_url, downloads / KAFKA_ARCHIVE)
    fetch(kafka_url + ".sha512", downloads / (KAFKA_ARCHIVE + ".sha512"))
    hashes[KAFKA_ARCHIVE] = {"algorithm": "sha512", "value": verify_published_sha512(
        downloads / KAFKA_ARCHIVE, downloads / (KAFKA_ARCHIVE + ".sha512"))}
    ecj_url = "https://repo.maven.apache.org/maven2/org/eclipse/jdt/ecj/3.33.0/" + ECJ
    for name, url in ((CONNECTOR, connector_url), (ECJ, ecj_url)):
        fetch(url, downloads / name)
        fetch(url + ".sha1", downloads / (name + ".sha1"))
        artifact_bytes = (downloads / name).read_bytes()
        expected = (downloads / (name + ".sha1")).read_text().strip().lower()
        actual = hashlib.sha1(artifact_bytes).hexdigest()
        if not re.fullmatch("[0-9a-f]{40}", expected) or actual != expected:
            raise ValueError(f"published SHA-1 mismatch: {name}")
        hashes[name] = {"algorithm": "sha1", "value": actual,
                        "local_sha256": hashlib.sha256(artifact_bytes).hexdigest()}
    kafka = runtime / f"kafka_2.13-{KAFKA_VERSION}"
    if not kafka.exists():
        with tarfile.open(downloads / KAFKA_ARCHIVE) as archive:
            # Refuse traversal and links outside the isolated runtime directory.
            for member in archive.getmembers():
                if not (runtime / member.name).resolve().is_relative_to(runtime):
                    raise ValueError("unsafe archive member")
                if member.issym() or member.islnk():
                    raise ValueError("links are not expected in Kafka archive")
            archive.extractall(runtime, filter="data")
    flink = Path(subprocess.check_output([str(python), "-c",
        "import pyflink,pathlib;print(pathlib.Path(pyflink.__file__).parent)"], text=True).strip())
    shutil.copy2(downloads / CONNECTOR, flink / "lib" / CONNECTOR)
    manifest = {"runtime_dir": str(runtime), "python": str(python), "kafka_home": str(kafka),
                "flink_home": str(flink), "versions": {"kafka": KAFKA_VERSION, "flink": FLINK_VERSION,
                "connector": "3.3.0-1.20"}, "published_checksums": hashes}
    (runtime / "runtime.json").write_text(json.dumps(manifest, indent=2) + "\n")
    freeze = subprocess.check_output(["uv", "pip", "freeze", "--python", str(python)], text=True)
    (runtime / "dependency-freeze.txt").write_text(freeze)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
