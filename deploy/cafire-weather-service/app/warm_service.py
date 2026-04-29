from __future__ import annotations

from .config import settings
from .warm import MeteogramWarmManager


def main() -> None:
    settings.ensure_dirs()
    manager = MeteogramWarmManager(settings)
    if not settings.meteogram_warm_enabled:
        print("meteogram warmer disabled", flush=True)
        return
    print("starting meteogram warmer", flush=True)
    manager.run_forever()


if __name__ == "__main__":
    main()
