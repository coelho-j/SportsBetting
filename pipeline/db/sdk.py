# pipeline/db/sdk.py: SDK for Interacting with Baseball DuckDB Database

import duckdb
import pybaseball as pyb
import pandas as pd
from typing import Optional, List
from .models.batting import PlayerBattingStats, TeamBattingStats
from .models.pitching import PlayerPitchingStats
from .sql.pybaseball.db import CREATE_SCHEMAS
from .sql.pybaseball.raw import (
    CREATE_TEAM_BATTING, CREATE_TEAM_PITCHING, CREATE_GAME_LOGS,
    CREATE_PLAYER_BATTING, CREATE_PLAYER_PITCHING
)
from .sql.pybaseball.processed import (
    CREATE_PROCESSED_TEAM_BATTING, CREATE_PROCESSED_TEAM_PITCHING,
    CREATE_PROCESSED_PLAYER_BATTING, CREATE_PROCESSED_PLAYER_PITCHING
)
from .sql.pybaseball.features import (
    CREATE_FEATURES_TEAM_FEATURES, CREATE_FEATURES_PLAYER_FEATURES
)
import os

# Global persistent connection (singleton-like for no cold starts)
_connection = None

def _get_connection() -> duckdb.DuckDBPyConnection:
    """Lazily initialize and return persistent connection, ensuring tables exist."""
    global _connection
    if _connection is None:
        os.makedirs('./data', exist_ok=True)
        _connection = duckdb.connect(database='./data/baseball.duckdb', read_only=False)
        # Ensure schemas and all tables exist
        _connection.execute(CREATE_SCHEMAS)
        _connection.execute(CREATE_TEAM_BATTING)
        _connection.execute(CREATE_TEAM_PITCHING)
        _connection.execute(CREATE_GAME_LOGS)
        _connection.execute(CREATE_PLAYER_BATTING)
        _connection.execute(CREATE_PLAYER_PITCHING)
        _connection.execute(CREATE_PROCESSED_TEAM_BATTING)
        _connection.execute(CREATE_PROCESSED_TEAM_PITCHING)
        _connection.execute(CREATE_PROCESSED_PLAYER_BATTING)
        _connection.execute(CREATE_PROCESSED_PLAYER_PITCHING)
        _connection.execute(CREATE_FEATURES_TEAM_FEATURES)
        _connection.execute(CREATE_FEATURES_PLAYER_FEATURES)
    return _connection

def close_connection():
    """Close the persistent connection."""
    global _connection
    if _connection:
        _connection.close()
        _connection = None

# Helper: Get player ID from name (using pybaseball)
def get_id_from_name(player_name: str) -> Optional[int]:
    """Lookup MLBAM player ID from name (fuzzy match)."""
    try:
        parts = player_name.split()
        if len(parts) < 2:
            return None
        last_name = parts[-1].lower()
        first_name = ' '.join(parts[:-1]).lower()  # Handle middle names if present
        lookup_df = pyb.playerid_lookup(last_name, first_name, fuzzy=True)
        if not lookup_df.empty:
            return int(lookup_df['key_mlbam'].iloc[0])
    except Exception as e:
        print(f"Error looking up player ID for '{player_name}': {e}")
    return None

# SDK Module: data.baseball
class BaseballSDK:
    """High-level interface for baseball data operations."""

    @staticmethod
    def get_player(player_name: str, season: int = 2024) -> Optional[PlayerBattingStats]:
        """Fetch player batting stats by name; inserts to DB if missing."""
        player_id = get_id_from_name(player_name)
        if not player_id:
            return None

        con = _get_connection()
        # Check if exists in DB
        result = con.execute(
            "SELECT * FROM processed.pybaseball_player_batting WHERE player_id = ? AND season = ?",
            [player_id, season]
        ).fetchdf()
        if not result.empty:
            return PlayerBattingStats.from_df(result)

        # Fetch from pybaseball and insert
        try:
            # Lookup for FanGraphs ID
            parts = player_name.split()
            last_name = parts[-1].lower()
            first_name = ' '.join(parts[:-1]).lower()
            lookup_df = pyb.playerid_lookup(last_name, first_name, fuzzy=True)
            if lookup_df.empty:
                raise ValueError(f"No lookup found for {player_name}")
            idfg = lookup_df['key_fangraphs'].iloc[0]

            # Fetch full season stats and filter
            stats_df = pyb.batting_stats(season, ind=1, qual=0)
            if stats_df.empty:
                raise ValueError("No batting stats available for season")
            player_stats = stats_df[stats_df['IDfg'] == idfg]
            if player_stats.empty:
                raise ValueError(f"No stats found for player ID {idfg}")

            # Prepare for insert (take first matching row, rename columns)
            player_row = player_stats.iloc[0:1].copy()
            player_row = player_row.rename(columns={
                '2B': 'double', '3B': 'triple', 'HR': 'hr', 'RBI': 'rbi', 'SB': 'sb',
                'BB': 'bb', 'SO': 'so', 'HBP': 'hbp', 'AVG': 'avg', 'OBP': 'obp',
                'SLG': 'slg', 'OPS': 'ops', 'Team': 'team', 'Name': 'player_name',
                'G': 'g', 'PA': 'pa', 'AB': 'ab', 'R': 'r', 'H': 'h', 'Age': 'age', 'CS': 'cs'
            })
            print("BEFORE GIANT BLOCK FOR .gets and after rename")
            print(player_row)
            player_row['player_id'] = player_id
            player_row['season'] = season  # Will be added in INSERT
            player_row['idfg'] = idfg
            player_row['age'] = player_row.get('age', 0)  # Use renamed column
            player_row['g'] = player_row.get('g', 0)
            player_row['pa'] = player_row.get('pa', 0)
            player_row['ab'] = player_row.get('ab', 0)
            player_row['r'] = player_row.get('r', 0)
            player_row['h'] = player_row.get('h', 0)
            player_row['double'] = player_row.get('double', 0)  # Use renamed column
            player_row['triple'] = player_row.get('triple', 0)
            player_row['hr'] = player_row.get('hr', 0)
            player_row['rbi'] = player_row.get('rbi', 0)
            player_row['sb'] = player_row.get('sb', 0)
            player_row['cs'] = player_row.get('cs', 0)
            player_row['bb'] = player_row.get('bb', 0)
            player_row['so'] = player_row.get('so', 0)
            player_row['hbp'] = player_row.get('hbp', 0)
            player_row['avg'] = player_row.get('avg', 0.0)
            player_row['obp'] = player_row.get('obp', 0.0)
            player_row['slg'] = player_row.get('slg', 0.0)
            player_row['ops'] = player_row.get('ops', 0.0)

            print(f"Post-rename for {player_name}:")
            print(player_row[['player_name', 'team', 'age', 'g', 'pa', 'ab', 'h', 'hr', 'rbi', 'bb', 'so', 'avg', 'obp', 'slg', 'ops']].to_dict('records'))

            # Explicitly convert rates to float (handles any string/NaN issues)
            for col in ['avg', 'obp', 'slg', 'ops']:
                player_row[col] = pd.to_numeric(player_row[col], errors='coerce').fillna(0.0)
            # Insert raw
            con.register('temp_stats', player_row)
            con.execute("""
                INSERT INTO raw.pybaseball_player_batting 
                SELECT season, player_id, player_name, team, idfg, age, g, pa, ab, r, h, double, triple, hr, rbi, sb, cs, bb, so, hbp, avg, obp, slg, ops 
                FROM temp_stats;
            """)
            con.unregister('temp_stats')
            # Re-run processed and features tables
            con.execute(CREATE_PROCESSED_PLAYER_BATTING)
            con.execute(CREATE_FEATURES_PLAYER_FEATURES)
            # Fetch processed
            result = con.execute(
                "SELECT * FROM processed.pybaseball_player_batting WHERE player_id = ? AND season = ?",
                [player_id, season]
            ).fetchdf()
            if not result.empty:
                return PlayerBattingStats.from_df(result)
        except Exception as e:
            print(f"Error fetching/inserting player stats for '{player_name}' (ID {player_id}): {e}")
        return None

    @staticmethod
    def get_team_batting(team_abbr: str, season: int = 2024) -> Optional[TeamBattingStats]:
        """Fetch team batting stats by abbreviation."""
        con = _get_connection()
        result = con.execute(
            "SELECT * FROM processed.pybaseball_team_batting WHERE team = ? AND season = ?",
            [team_abbr, season]
        ).fetchdf()
        if not result.empty:
            return TeamBattingStats.from_row(result.iloc[0].to_dict())
        return None

    @staticmethod
    def execute_query(query: str, params: List = None) -> pd.DataFrame:
        """Execute custom SQL query."""
        con = _get_connection()
        if params:
            return con.execute(query, params).fetchdf()
        return con.execute(query).fetchdf()

# Usage: from pipeline.db.sdk import BaseballSDK; player = BaseballSDK.get_player('Mike Trout')