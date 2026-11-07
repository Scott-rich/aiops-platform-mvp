"""Official SMD entity loader; machine indices are not service identities."""

from pathlib import Path

import numpy as np

from research.datasets.base import DatasetTuple, clean, validate_labels
from research.experiment import sha256


class SMD:
    def __init__(self, root: Path, entity: str) -> None:
        if not entity.startswith("machine-") or "/" in entity or ".." in entity:
            raise ValueError("invalid machine name")
        self.root, self.entity = root, entity

    def load(self) -> DatasetTuple:
        paths = [
            self.root / split / f"{self.entity}.txt" for split in ("train", "test", "test_label")
        ]
        train, test = [np.loadtxt(p, delimiter=",", ndmin=2) for p in paths[:2]]
        raw_labels = np.loadtxt(paths[2], delimiter=",").reshape(-1)
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
                "dataset": "SMD",
                "entity": self.entity,
                "source_kind": "public",
                "dimensions": [f"anonymous_kpi_{i}" for i in range(train.shape[1])],
                "sampling_interval_seconds": None,
                "labels": "point_binary; train assumed normal",
                "sha256": {str(p): sha256(p) for p in paths},
                "preprocessing": info,
            },
        )
