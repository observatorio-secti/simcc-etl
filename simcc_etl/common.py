from dataclasses import dataclass


@dataclass(slots=True)
class Stats:
    found: int = 0
    succeeded: int = 0
    failed: int = 0
