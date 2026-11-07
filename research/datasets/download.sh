#!/usr/bin/env sh
set -eu
python -m scripts.research.download --dataset all --output "${1:?supply a new output directory}"
