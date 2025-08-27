# pipeline/db/sql/pybaseball/raw.py

# RAW LAYER: Direct API dumps
CREATE_TEAM_BATTING = """
CREATE TABLE IF NOT EXISTS raw.pybaseball_team_batting (
    season INTEGER,
    team VARCHAR,
    g INTEGER,
    ab INTEGER,
    r INTEGER,
    h INTEGER,
    hr INTEGER,
    rbi INTEGER,
    sb INTEGER,
    obp DECIMAL(5,3),
    slg DECIMAL(5,3),
    PRIMARY KEY (season, team)
);
"""

CREATE_TEAM_PITCHING = """
CREATE TABLE IF NOT EXISTS raw.pybaseball_team_pitching (
    season INTEGER,
    team VARCHAR,
    w INTEGER,
    l INTEGER,
    era DECIMAL(5,2),
    ip DECIMAL(10,1),
    so INTEGER,
    whip DECIMAL(5,2),
    fip DECIMAL(5,2),
    PRIMARY KEY (season, team)
);
"""

CREATE_GAME_LOGS = """
CREATE TABLE IF NOT EXISTS raw.pybaseball_game_logs (
    season INTEGER,
    team VARCHAR,
    date DATE,
    opp VARCHAR,
    wl VARCHAR,
    r DECIMAL(5,1),
    ra DECIMAL(5,1),
    inn DECIMAL(5,1),
    gb VARCHAR,
    home_away VARCHAR,
    PRIMARY KEY (season, team, opp, date)
);
"""


CREATE_PLAYER_BATTING = """
CREATE TABLE IF NOT EXISTS raw.pybaseball_player_batting (
    season INTEGER,
    player_id INTEGER,  -- MLBAM ID
    player_name VARCHAR,
    team VARCHAR,
    idfg VARCHAR,  -- FanGraphs ID
    age INTEGER,
    g INTEGER,
    pa INTEGER,
    ab INTEGER,
    r INTEGER,
    h INTEGER,
    double INTEGER,
    triple INTEGER,
    hr INTEGER,
    rbi INTEGER,
    sb INTEGER,
    cs INTEGER,
    bb INTEGER,
    so INTEGER,
    hbp INTEGER,
    avg DECIMAL(5,3),
    obp DECIMAL(5,3),
    slg DECIMAL(5,3),
    ops DECIMAL(5,3),
    PRIMARY KEY (season, player_id)
);
"""

CREATE_PLAYER_PITCHING = """
CREATE TABLE IF NOT EXISTS raw.pybaseball_player_pitching (
    season INTEGER,
    player_id INTEGER,  -- MLBAM ID
    player_name VARCHAR,
    team VARCHAR,
    idfg VARCHAR,
    age INTEGER,
    w INTEGER,
    l INTEGER,
    era DECIMAL(5,2),
    g INTEGER,
    gs INTEGER,
    ip DECIMAL(10,1),
    h INTEGER,
    r INTEGER,
    er INTEGER,
    bb INTEGER,
    so INTEGER,
    whip DECIMAL(5,2),
    era_plus INTEGER,
    PRIMARY KEY (season, player_id)
);
"""