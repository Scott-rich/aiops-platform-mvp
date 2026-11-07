"""Official PSM loader; the timestamp column is excluded from model features."""

from pathlib import Path

import numpy as np

from research.datasets.base import DatasetTuple, clean, validate_labels
from research.experiment import sha256


class PSM:
    def __init__(self, root: Path) -> None:
        self.root = root

    def load(self) -> DatasetTuple:
        paths = [self.root / f"{name}.csv" for name in ("train", "test", "test_label")]
        train, test = [np.genfromtxt(p, delimiter=",", skip_header=1)[:, 1:] for p in paths[:2]]
        raw_labels = np.genfromtxt(paths[2], delimiter=",", skip_header=1)[:, -1]
        if not np.isin(raw_labels, [0, 1]).all():
            raise ValueError("invalid raw labels")
        labels = raw_labels.astype(np.int64)
        train, test, info = clean(train, test)
        validate_labels(labels, len(test))
        return (
            train,
            np.zeros(len(train), dtype=np.int64),
            test,
            labels,
            {
                "dataset": "PSM",
                "entity": "pooled",
                "source_kind": "public",
                "dimensions": paths[0].read_text().splitlines()[0].split(",")[1:],
                "sampling_interval_seconds": None,
                "labels": "point_binary; train assumed normal",
                "sha256": {str(p): sha256(p) for p in paths},
                "preprocessing": info,
            },
        )
