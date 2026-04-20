import gzip

import pandas as pd


def _append_sample_value(sample_row, key, value):
    """Append value to key, preserving repeated fields."""
    if not value:
        return
    if key in sample_row and sample_row[key]:
        sample_row[key] = f"{sample_row[key]} || {value}"
    else:
        sample_row[key] = value


def parse_soft_file(file_path):
    """Parse GEO SOFT family file into CSV file"""
    samples = []
    current_sample = None

    with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("^SAMPLE ="):
                if current_sample is not None:
                    samples.append(current_sample)

                sample_id = line.split("=", 1)[1].strip()
                current_sample = {"sample_id": sample_id}
                continue

            if current_sample is None:
                continue

            if line.startswith("^") and not line.startswith("^SAMPLE ="):
                samples.append(current_sample)
                current_sample = None
                continue

            if not line.startswith("!Sample_"):
                continue

            left, _, right = line.partition("=")
            key = left.strip()[len("!Sample_") :]
            value = right.strip()
            _append_sample_value(current_sample, key, value)

    if current_sample is not None:
        samples.append(current_sample)

    return pd.DataFrame(samples)
