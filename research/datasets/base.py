"""Common dataset contract and train-only missing value handling."""

from __future__ import annotations

from typing import Any, Protocol

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
DatasetTuple = tuple[FloatArray, IntArray, FloatArray, IntArray, dict[str, Any]]


class Dataset(Protocol):
    def load(self) -> DatasetTuple:
        """Return train values/labels, test values/labels, and metadata."""
        ...


def clean(train: FloatArray, test: FloatArray) -> tuple[FloatArray, FloatArray, dict[str, Any]]:
    """Impute nonfinite values using train medians; never inspect test statistics."""
    if train.ndim != 2 or test.ndim != 2 or train.shape[1] != test.shape[1]:
        raise ValueError("train/test must be aligned 2D matrices")
    if not len(train) or not len(test):
        raise ValueError("empty dataset")
    medians = np.array(
        [np.median(c[np.isfinite(c)]) if np.any(np.isfinite(c)) else 0.0 for c in train.T]
    )
    info = {
        "train_imputed": int((~np.isfinite(train)).sum()),
        "test_imputed": int((~np.isfinite(test)).sum()),
        "imputation": "train_column_median",
        "all_missing_train_columns": np.where(~np.isfinite(train).any(axis=0))[0].tolist(),
    }
    return (
        np.where(np.isfinite(train), train, medians),
        np.where(np.isfinite(test), test, medians),
        info,
    )


def validate_labels(labels: IntArray, n: int) -> None:
    if labels.shape != (n,) or not np.isin(labels, [0, 1]).all():
        raise ValueError("labels must be binary and aligned to test rows")
