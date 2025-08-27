from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class PlayerBattingStats:
    """Represents player batting statistics for a season."""
    season: int
    player_id: int
    player_name: str
    team: str
    age: int
    g: int  # Games
    pa: int  # Plate Appearances
    ab: int  # At Bats
    r: int   # Runs
    h: int   # Hits
    double: int
    triple: int
    hr: int  # Home Runs
    rbi: int  # Runs Batted In
    sb: int  # Stolen Bases
    bb: int  # Walks
    so: int  # Strikeouts
    avg: float
    obp: float
    slg: float
    ops: float

    @classmethod
    def from_dict(cls, row: dict) -> 'PlayerBattingStats':
        """Create instance from a dictionary (e.g., from DB row)."""
        return cls(**{k: row.get(k, 0) for k in cls.__dataclass_fields__})

    @classmethod
    def from_df(cls, df: pd.DataFrame, row_idx: int = 0) -> Optional['PlayerBattingStats']:
        """Create from DataFrame row."""
        if df.empty:
            return None
        row = df.iloc[row_idx].to_dict()
        # Map if needed (e.g., '2B' -> 'double')
        row['double'] = row.get('2B', row.get('double', 0))
        row.pop('2B', None)
        row['triple'] = row.get('3B', row.get('triple', 0))
        row.pop('3B', None)

        # Some special stats
        row['avg'] = row.get('avg_clean', row.get('avg', 0))
        row.pop('avg_clean', None)

        row['obp'] = row.get('obp_clean', row.get('obp', 0))
        row.pop('obp_clean', None)

        row['slg'] = row.get('slg_clean', row.get('slg', 0))
        row.pop('slg_clean', None)

        row['ops'] = row.get('ops_clean', row.get('ops', 0))
        row.pop('ops_clean', None)
        return cls.from_dict(row)  # Fixed: Call from_dict instead of from_row


@dataclass
class TeamBattingStats:
    """Represents team batting statistics for a season."""
    season: int
    team: str
    g: int
    ab: int
    r: int
    h: int
    hr: int
    rbi: int
    sb: int
    obp: float
    slg: float

    @classmethod
    def from_row(cls, row: dict) -> 'TeamBattingStats':
        return cls(**{k: row.get(k, 0) for k in cls.__dataclass_fields__})