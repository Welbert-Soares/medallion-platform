"""`medallion-validate-config [DIR]`: validate the whole config directory (default: config/platform)."""

import sys

from medallion.controller.config.loader import ConfigError, load_config

DEFAULT_CONFIG_DIR = "config/platform"


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    config_dir = args[0] if args else DEFAULT_CONFIG_DIR
    try:
        config = load_config(config_dir)
    except ConfigError as e:
        print(f"{config_dir}: {e}", file=sys.stderr)
        return 1
    structure = config.catalog_structure
    print(
        f"{config_dir}: OK — {len(structure.layers)} layers, "
        f"{len(structure.source_systems)} source systems, "
        f"{len(config.connections)} connections, {len(config.ingestions)} ingestions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
