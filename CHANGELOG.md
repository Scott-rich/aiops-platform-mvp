# Changelog

All changes below were made in this upgrade session; historical authorship is not reconstructed.

## [0.3.0-research] - 2026-11-04
### Added
- Train-only SMD/PSM evaluation, uncertainty estimates, reproducibility hashes and negative comparisons.
- Frozen synthetic RCA labels, two real local-model measurement protocols, calibration and paired injection tests.
- Versioned API aliases, bounded token buckets and Prometheus self-observation.
- Blocking code-quality configuration and Kubernetes/Chaos experiment tooling.
### Changed
- Runtime dependency/image separation; explicit legacy type boundaries and exception handling.
### Compatibility
- Original assertions and validator thresholds remain unchanged. Legacy routes preserve request semantics by default; opt-in GET redirect is documented.
### Limitations
- Cluster execution, clean-container demonstration and remote CI must be distinguished from local evidence. Human verification of golden labels is still absent.
