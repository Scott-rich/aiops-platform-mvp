"""Four baselines exposing fit, score, and decide with train-only calibration."""

from collections.abc import Callable

from research.detectors.base import Detector
from research.detectors.spot import SPOT
from research.detectors.statistical import MAD, IForest, ZScore


def create(name: str, seed: int = 0) -> Detector:
    factories: dict[str, Callable[[int], Detector]] = {
        "zscore": ZScore,
        "mad": MAD,
        "iforest": IForest,
        "spot": SPOT,
    }
    if name not in factories:
        raise ValueError(f"unknown detector: {name}")
    return factories[name](seed)
