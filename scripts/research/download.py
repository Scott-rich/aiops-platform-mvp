"""Download immutable official dataset revisions and retain a SHA256 manifest."""

from __future__ import annotations

import argparse
import logging
import urllib.request
from pathlib import Path

from research.experiment import Experiment, sha256, write_json

SMD_REV = "7fb0e0acf89ea49908896bcc9f9e80fcfff6baf4"
PSM_REV = "948e61eb579815ff2a6bce721332d8220d59ca47"
ENTITIES = ["machine-1-1", "machine-1-2", "machine-1-3", "machine-1-4", "machine-1-5"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset", choices=["smd", "psm", "all"], default="all")
    args = parser.parse_args()
    urls: list[tuple[str, str]] = []
    if args.dataset in {"smd", "all"}:
        urls.extend(
            (
                f"smd/{split}/{e}.txt",
                f"https://raw.githubusercontent.com/NetManAIOps/"
                f"OmniAnomaly/{SMD_REV}/ServerMachineDataset/{split}/{e}.txt",
            )
            for e in ENTITIES
            for split in ("train", "test", "test_label")
        )
    if args.dataset in {"psm", "all"}:
        urls.extend(
            (
                f"psm/{name}.csv",
                f"https://raw.githubusercontent.com/eBay/RANSynCoders/{PSM_REV}/data/{name}.csv",
            )
            for name in ("train", "test", "test_label")
        )
    with Experiment(
        args.output,
        {
            "dataset": args.dataset,
            "sampling": "first five SMD machine indices, chosen before inspecting scores",
            "revisions": {"smd": SMD_REV, "psm": PSM_REV},
        },
        [],
    ) as run:
        files = []
        for relative, url in urls:
            destination = run.output / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            logging.info("download %s", url)
            with (
                urllib.request.urlopen(url, timeout=120) as response,
                destination.open("xb") as stream,
            ):
                while block := response.read(1024 * 1024):
                    stream.write(block)
            files.append(
                {
                    "path": relative,
                    "url": url,
                    "sha256": sha256(destination),
                    "bytes": destination.stat().st_size,
                }
            )
        write_json(run.output / "sha256.json", files)
        (run.output / "SHA256SUMS").write_text(
            "".join(f"{f['sha256']}  {f['path']}\n" for f in files)
        )
        run.result["files"] = files
        run.result["metrics"] = {"downloaded_files": len(files)}
        run.summary(
            [
                "下载官方固定 revision 数据，未做模型评估。",
                "见 files 和 SHA256SUMS。",
                "无算法对比。",
                "哈希验证字节身份，不验证标签正确性。",
                "SMD 匿名 KPI；PSM 归池实体；SWaT 未申请。",
                "以训练集校准检测器并保留所有结果。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
