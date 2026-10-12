#!/usr/bin/env python3
"""Simulate a DataOps promotion of the scrm-etl library into one environment."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path


def load_simple_yaml(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip().strip("\"'")
    return data


def find_wheel(artifact_dir: Path) -> Path:
    wheels = sorted(artifact_dir.rglob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"No wheel found under {artifact_dir}")
    return wheels[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote scrm-etl into a target environment")
    parser.add_argument("--env", required=True, choices=("DEV", "PROD"))
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--release-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_simple_yaml(args.config)
    if config.get("environment") != args.env:
        raise ValueError(f"Config {args.config} is not for {args.env}: {config}")

    wheel = find_wheel(args.artifact_dir)
    lib_dir = args.release_dir / "lib"
    cfg_dir = args.release_dir / "config"
    lib_dir.mkdir(parents=True, exist_ok=True)
    cfg_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(wheel, lib_dir / wheel.name)
    shutil.copy2(args.config, cfg_dir / "environment.yaml")

    manifest = {
        "environment": args.env,
        "artifact": wheel.name,
        "deployed_at_utc": datetime.now(UTC).isoformat(),
        "build_id": os.getenv("BUILD_BUILDID", "local"),
        "source_branch": os.getenv("BUILD_SOURCEBRANCH", "local"),
        "source_version": os.getenv("BUILD_SOURCEVERSION", "local"),
        "target": config,
        "release_dir": str(args.release_dir),
        "command": [
            "python",
            "-m",
            "scrm_etl",
            "--mode",
            config["mode"],
            "--output",
            config["output_path"],
        ],
    }
    manifest_path = args.release_dir / "deploy-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"Deployed {wheel.name} to {args.env}")
    print(f"Catalog: {config['catalog']}.{config['schema']}.{config['table']}")
    print(f"Output:  {config['output_path']}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
