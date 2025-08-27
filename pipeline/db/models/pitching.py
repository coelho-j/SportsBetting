# pipeline/db/models/pitching.py

from dataclasses import dataclass
# from typing import Optional
# import pandas as pd

@dataclass
class PlayerPitchingStats:
    """Represents player pitching statistics for a season."""
    season: int
    player_id: int
    player_name: str
    team: str
    age: int
    w: int
    l: int
    era: float
    g: int
    gs: int  # Games Started
    ip: float  # Innings Pitched
    so: int
    whip: float

    @classmethod
    def from_row(cls, row: dict) -> 'PlayerPitchingStats':
        return cls(**{k: row.get(k, 0) for k in cls.__dataclass_fields__})