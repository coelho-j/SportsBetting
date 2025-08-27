# pipeline/db/__init__.py: Database Initialization and Setup

import duckdb
import pandas as pd
import pybaseball as pyb
import os
import sys
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

def setup():
    """Initialize the DuckDB database: create schemas, tables, and insert sample data sourced from pybaseball."""
    os.makedirs('./data', exist_ok=True)
    con = duckdb.connect(database='./data/baseball.duckdb', read_only=False)

    # Enable pybaseball cache for efficiency
    pyb.cache.enable()

    # Create schemas
    con.execute(CREATE_SCHEMAS)

    # Create raw tables
    con.execute(CREATE_TEAM_BATTING)
    con.execute(CREATE_TEAM_PITCHING)
    con.execute(CREATE_GAME_LOGS)
    con.execute(CREATE_PLAYER_BATTING)
    con.execute(CREATE_PLAYER_PITCHING)

    # Insert limited team batting data (first 5 rows from 2024)
    team_batting = pyb.team_batting(2024)[['Team', 'G', 'AB', 'R', 'H', 'HR', 'RBI', 'SB', 'OBP', 'SLG']].head(5)
    team_batting = team_batting.rename(columns={
        'Team': 'team', 'G': 'g', 'AB': 'ab', 'R': 'r', 'H': 'h', 'HR': 'hr', 
        'RBI': 'rbi', 'SB': 'sb', 'OBP': 'obp', 'SLG': 'slg'
    })  # Align with table schema (lowercase)
    if not team_batting.empty:
        con.register('temp_batting', team_batting)
        con.execute("""
            UPDATE raw.pybaseball_team_batting 
            SET g = temp_batting.g, ab = temp_batting.ab, r = temp_batting.r, h = temp_batting.h, 
                hr = temp_batting.hr, rbi = temp_batting.rbi, sb = temp_batting.sb, obp = temp_batting.obp, slg = temp_batting.slg
            FROM temp_batting 
            WHERE raw.pybaseball_team_batting.season = 2024 AND raw.pybaseball_team_batting.team = temp_batting.team;
        """)
        con.execute("""
            INSERT INTO raw.pybaseball_team_batting 
            SELECT 2024 AS season, team, g, ab, r, h, hr, rbi, sb, obp, slg 
            FROM temp_batting 
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.pybaseball_team_batting 
                WHERE season = 2024 AND team = temp_batting.team
            );
        """)
        con.unregister('temp_batting')
    else:
        print("Warning: No team batting data retrieved from pybaseball.")

    # Insert limited team pitching data (first 5 rows from 2024)
    team_pitching = pyb.team_pitching(2024)[['Team', 'W', 'L', 'ERA', 'IP', 'SO', 'WHIP', 'FIP']].head(5)
    team_pitching = team_pitching.rename(columns={
        'Team': 'team', 'W': 'w', 'L': 'l', 'ERA': 'era', 'IP': 'ip', 
        'SO': 'so', 'WHIP': 'whip', 'FIP': 'fip'
    })
    if not team_pitching.empty:
        con.register('temp_pitching', team_pitching)
        con.execute("""
            UPDATE raw.pybaseball_team_pitching 
            SET w = temp_pitching.w, l = temp_pitching.l, era = temp_pitching.era, ip = temp_pitching.ip, 
                so = temp_pitching.so, whip = temp_pitching.whip, fip = temp_pitching.fip
            FROM temp_pitching 
            WHERE raw.pybaseball_team_pitching.season = 2024 AND raw.pybaseball_team_pitching.team = temp_pitching.team;
        """)
        con.execute("""
            INSERT INTO raw.pybaseball_team_pitching 
            SELECT 2024 AS season, team, w, l, era, ip, so, whip, fip 
            FROM temp_pitching 
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.pybaseball_team_pitching 
                WHERE season = 2024 AND team = temp_pitching.team
            );
        """)
        con.unregister('temp_pitching')
    else:
        print("Warning: No team pitching data retrieved from pybaseball.")

    # Insert limited game logs (first 5 games for LAD from 2024)
    dodgers_logs = pyb.schedule_and_record(2024, 'LAD')[['Date', 'Opp', 'W/L', 'R', 'RA', 'Inn', 'GB', 'Home_Away']].head(5)
    if not dodgers_logs.empty:
        # Standardize date by appending year and parsing
        dodgers_logs['Date'] = pd.to_datetime(dodgers_logs['Date'] + ', 2024').dt.date
        dodgers_logs = dodgers_logs.rename(columns={
            'W/L': 'wl', 'R': 'r', 'RA': 'ra', 'Inn': 'inn', 'GB': 'gb', 
            'Home_Away': 'home_away', 'Opp': 'opp'
        })
        con.register('temp_logs', dodgers_logs)
        con.execute("""
            UPDATE raw.pybaseball_game_logs 
            SET opp = temp_logs.opp, wl = temp_logs.wl, r = temp_logs.r, ra = temp_logs.ra, 
                inn = temp_logs.inn, gb = temp_logs.gb, home_away = temp_logs.home_away
            FROM temp_logs 
            WHERE raw.pybaseball_game_logs.season = 2024 
                AND raw.pybaseball_game_logs.team = 'LAD' 
                AND raw.pybaseball_game_logs.date = temp_logs.date;
        """)
        con.execute("""
            INSERT INTO raw.pybaseball_game_logs 
            SELECT 2024 AS season, 'LAD' AS team, date, opp, wl, r, ra, inn, gb, home_away 
            FROM temp_logs 
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.pybaseball_game_logs 
                WHERE season = 2024 AND team = 'LAD' AND date = temp_logs.date
            );
        """)
        con.unregister('temp_logs')
    else:
        print("Warning: No game logs retrieved from pybaseball.")

    # Insert player batting data for Mike Trout from 2024
    try:
        lookup = pyb.playerid_lookup('trout', 'mike')
        if not lookup.empty:
            player_id = int(lookup['key_mlbam'].iloc[0])
            idfg = int(lookup['key_fangraphs'].iloc[0])
            stats_df = pyb.batting_stats(2024, ind=1, qual=0)
            if not stats_df.empty:
                player_stats = stats_df[stats_df['IDfg'] == idfg]
                if not player_stats.empty:
                    player_row = player_stats.iloc[0:1].copy()
                    player_row = player_row.rename(columns={
                        '2B': 'double', '3B': 'triple', 'HR': 'hr', 'RBI': 'rbi', 'SB': 'sb', 
                        'BB': 'bb', 'SO': 'so', 'HBP': 'hbp', 'AVG': 'avg', 'OBP': 'obp', 
                        'SLG': 'slg', 'OPS': 'ops', 'Team': 'team', 'Name': 'player_name', 
                        'G': 'g', 'PA': 'pa', 'AB': 'ab', 'R': 'r', 'H': 'h', 'Age': 'age', 'CS': 'cs'
                    })
                    player_row['player_id'] = player_id
                    player_row['idfg'] = idfg
                    # Convert rate strings to floats (handles '.220' format from pybaseball)
                    for col in ['avg', 'obp', 'slg', 'ops']:
                        player_row[col] = pd.to_numeric(player_row[col], errors='coerce').fillna(0.0)
                    
                    con.register('temp_player_batting', player_row)
                    con.execute("""
                        UPDATE raw.pybaseball_player_batting 
                        SET player_name = temp_player_batting.player_name, team = temp_player_batting.team, 
                            idfg = temp_player_batting.idfg, age = temp_player_batting.age, g = temp_player_batting.g, 
                            pa = temp_player_batting.pa, ab = temp_player_batting.ab, r = temp_player_batting.r, 
                            h = temp_player_batting.h, double = temp_player_batting.double, triple = temp_player_batting.triple, 
                            hr = temp_player_batting.hr, rbi = temp_player_batting.rbi, sb = temp_player_batting.sb, 
                            cs = temp_player_batting.cs, bb = temp_player_batting.bb, so = temp_player_batting.so, 
                            hbp = temp_player_batting.hbp, avg = temp_player_batting.avg, obp = temp_player_batting.obp, 
                            slg = temp_player_batting.slg, ops = temp_player_batting.ops
                        FROM temp_player_batting 
                        WHERE raw.pybaseball_player_batting.season = 2024 AND raw.pybaseball_player_batting.player_id = temp_player_batting.player_id;
                    """)
                    con.execute("""
                        INSERT INTO raw.pybaseball_player_batting 
                        SELECT 2024 AS season, player_id, player_name, team, idfg, age, g, pa, ab, r, h, double, triple, hr, rbi, sb, cs, bb, so, hbp, avg, obp, slg, ops 
                        FROM temp_player_batting 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM raw.pybaseball_player_batting 
                            WHERE season = 2024 AND player_id = temp_player_batting.player_id
                        );
                    """)
                    con.unregister('temp_player_batting')
                else:
                    print("Warning: No batting stats found for Mike Trout in 2024 data.")
            else:
                print("Warning: No batting stats data available for 2024.")
    except Exception as e:
        print(f"Warning: Could not insert player batting data: {e}")

    # Insert player pitching data for Mike Trout from 2024 (likely empty)
    try:
        if 'lookup' in locals() and not lookup.empty:
            player_id = int(lookup['key_mlbam'].iloc[0])
            idfg = str(lookup['key_fangraphs'].iloc[0])
            pitch_df = pyb.pitching_stats(2024, ind=1)
            if not pitch_df.empty:
                player_pitch = pitch_df[pitch_df['IDfg'] == idfg]
                if not player_pitch.empty:
                    player_pitch_row = player_pitch.iloc[0:1].copy()
                    player_pitch_row = player_pitch_row.rename(columns={
                        'Team': 'team', 'Name': 'player_name', 'W': 'w', 'L': 'l', 'ERA': 'era', 
                        'G': 'g', 'GS': 'gs', 'IP': 'ip', 'H': 'h', 'R': 'r', 'ER': 'er', 
                        'BB': 'bb', 'SO': 'so', 'WHIP': 'whip', 'ERA+': 'era_plus', 'Age': 'age'
                    })
                    player_pitch_row['player_id'] = player_id
                    player_pitch_row['idfg'] = idfg
                    con.register('temp_player_pitching', player_pitch_row)
                    con.execute("""
                        UPDATE raw.pybaseball_player_pitching 
                        SET player_name = temp_player_pitching.player_name, team = temp_player_pitching.team, 
                            idfg = temp_player_pitching.idfg, age = temp_player_pitching.age, w = temp_player_pitching.w, 
                            l = temp_player_pitching.l, era = temp_player_pitching.era, g = temp_player_pitching.g, 
                            gs = temp_player_pitching.gs, ip = temp_player_pitching.ip, h = temp_player_pitching.h, 
                            r = temp_player_pitching.r, er = temp_player_pitching.er, bb = temp_player_pitching.bb, 
                            so = temp_player_pitching.so, whip = temp_player_pitching.whip, era_plus = temp_player_pitching.era_plus
                        FROM temp_player_pitching 
                        WHERE raw.pybaseball_player_pitching.season = 2024 AND raw.pybaseball_player_pitching.player_id = temp_player_pitching.player_id;
                    """)
                    con.execute("""
                        INSERT INTO raw.pybaseball_player_pitching 
                        SELECT 2024 AS season, player_id, player_name, team, idfg, age, w, l, era, g, gs, ip, h, r, er, bb, so, whip, era_plus 
                        FROM temp_player_pitching 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM raw.pybaseball_player_pitching 
                            WHERE season = 2024 AND player_id = temp_player_pitching.player_id
                        );
                    """)
                    con.unregister('temp_player_pitching')
    except Exception as e:
        print(f"Warning: Could not insert player pitching data: {e}")

    # Create and populate processed/features tables
    con.execute(CREATE_PROCESSED_TEAM_BATTING)
    con.execute(CREATE_PROCESSED_TEAM_PITCHING)
    con.execute(CREATE_PROCESSED_PLAYER_BATTING)
    con.execute(CREATE_PROCESSED_PLAYER_PITCHING)
    con.execute(CREATE_FEATURES_TEAM_FEATURES)
    con.execute(CREATE_FEATURES_PLAYER_FEATURES)

    print("Database setup complete with limited 2024 data sourced from pybaseball.")

    con.close()

def verify():
    """Verify the integrity of the DuckDB database, checking for schema existence, table presence, and data population."""
    db_path = './data/baseball.duckdb'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found at {db_path}. Run setup() first.")
    
    try:
        con = duckdb.connect(database=db_path, read_only=True)
    except duckdb.IOException as e:
        if 'lock on file' in str(e):
            print("Close any services that may be using DuckDB")
            sys.exit(1)
        else:
            print(f"Error connecting to database: {e}")
            sys.exit(1)
    
    try:
        # Check schemas using information_schema (compatible with DuckDB)
        schemas = con.execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name != 'system';").fetchdf()
        required_schemas = ['raw', 'processed', 'features']
        for schema in required_schemas:
            if schema not in schemas['schema_name'].values:
                raise ValueError(f"Schema '{schema}' does not exist.")
        
        # Check raw tables and row counts (allow 0 if fetch failed, but warn)
        raw_tables = {
            'pybaseball_team_batting': 5,
            'pybaseball_team_pitching': 5,
            'pybaseball_game_logs': 5,
            'pybaseball_player_batting': 1,
            'pybaseball_player_pitching': 0
        }
        for table, expected_rows in raw_tables.items():
            row_count = con.execute(f"SELECT COUNT(*) AS count FROM raw.{table};").fetchone()[0]
            if row_count == 0:
                print(f"Warning: Table 'raw.{table}' has 0 rows; pybaseball fetch may have failed.")
            if row_count < expected_rows and row_count > 0:  # Raise only if partial data
                raise ValueError(f"Table 'raw.{table}' has {row_count} rows, expected at least {expected_rows}.")
        
        # Verify season is 2024 in non-empty raw tables
        for table in raw_tables.keys():
            seasons = con.execute(f"SELECT DISTINCT season FROM raw.{table};").fetchall()
            if seasons and not any(s[0] == 2024 for s in seasons):
                raise ValueError(f"Table 'raw.{table}' missing season 2024 data.")
        
        # Check processed tables (should match raw row counts)
        processed_tables = {
            'pybaseball_team_batting': 5,
            'pybaseball_team_pitching': 5,
            'pybaseball_player_batting': 1,
            'pybaseball_player_pitching': 0
        }
        for table, expected_rows in processed_tables.items():
            row_count = con.execute(f"SELECT COUNT(*) AS count FROM processed.{table};").fetchone()[0]
            if row_count == 0:
                print(f"Warning: Table 'processed.{table}' has 0 rows.")
            if row_count < expected_rows and row_count > 0:
                raise ValueError(f"Table 'processed.{table}' has {row_count} rows, expected at least {expected_rows}.")
        
        # Check features tables (non-empty if data present)
        features_team_rows = con.execute("SELECT COUNT(*) AS count FROM features.team_features;").fetchone()[0]
        if features_team_rows == 0:
            print("Warning: Table 'features.team_features' is empty; no processed data to derive from.")
        
        features_player_rows = con.execute("SELECT COUNT(*) AS count FROM features.player_features;").fetchone()[0]
        if features_player_rows == 0:
            print("Warning: Table 'features.player_features' is empty; no processed data to derive from.")
        
        # Sample data integrity checks (skip if no data)
        if con.execute("SELECT COUNT(*) FROM raw.pybaseball_team_batting;").fetchone()[0] > 0:
            lad_batting = con.execute("SELECT team, r FROM raw.pybaseball_team_batting WHERE team = 'LAD';").fetchone()
            if lad_batting and lad_batting[1] <= 0:
                raise ValueError("Invalid data in raw.pybaseball_team_batting for LAD (non-positive runs).")
        
        if con.execute("SELECT COUNT(*) FROM raw.pybaseball_game_logs;").fetchone()[0] > 0:
            lad_games = con.execute("SELECT COUNT(*) AS count FROM raw.pybaseball_game_logs WHERE team = 'LAD';").fetchone()[0]
            if lad_games != 5:
                print(f"Warning: Expected 5 game logs for LAD, found {lad_games}.")
        
        if con.execute("SELECT COUNT(*) FROM raw.pybaseball_player_batting;").fetchone()[0] > 0:
            trout_batting = con.execute("SELECT player_name, hr FROM raw.pybaseball_player_batting WHERE player_id = 545361;").fetchone()
            if not trout_batting or 'Trout' not in str(trout_batting[0]):
                raise ValueError("Player data integrity check failed for Mike Trout (batting).")
        
        print("Database integrity verified successfully: schemas and tables are correct. Data population may be partial if fetches failed.")
        
    finally:
        con.close()
    
    print("ok")

# Auto-run if module executed directly
if __name__ == "__main__":
    setup()