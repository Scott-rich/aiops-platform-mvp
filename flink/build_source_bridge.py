"""Compile the small Kafka metadata bridge using javac or an explicitly supplied ECJ.

python flink/build_source_bridge.py --flink-home /path/to/pyflink --connector /path/to/connector.jar \
    --output /tmp/aiops-kafka-source.jar [--ecj /tmp/ecj.jar]
No binary is committed to the project. Requires Java 11+ and matching Flink libs.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flink-home", required=True)
    parser.add_argument("--connector", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ecj")
    args = parser.parse_args()
    jars = list((Path(args.flink_home) / "lib").glob("*.jar")) + [Path(args.connector)]
    if not jars or any(not jar.is_file() for jar in jars):
        parser.error("Flink libs / connector not found")
    source = Path(__file__).resolve().parent / "java" / "aiops" / "SourceEnvelope.java"
    with tempfile.TemporaryDirectory(prefix="aiops-source-bridge-") as tmp:
        if args.ecj:
            compiler = ["java", "-jar", args.ecj, "-11", "-proc:none"]
        elif shutil.which("javac"):
            compiler = ["javac", "--release", "11"]
        else:
            parser.error("javac unavailable; supply --ecj /path/to/ecj.jar")
        subprocess.run(compiler + ["-encoding", "UTF-8", "-classpath", os.pathsep.join(map(str, jars)), "-d", tmp, str(source)], check=True, timeout=90)
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as bundle:
            for entry in Path(tmp).rglob("*.class"):
                bundle.write(entry, entry.relative_to(tmp))
        print(output)


if __name__ == "__main__":
    main()
