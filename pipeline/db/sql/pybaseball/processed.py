# PROCESSED LAYER: Cleaning and normalization
CREATE_PROCESSED_TEAM_BATTING = """
CREATE OR REPLACE TABLE processed.pybaseball_team_batting AS
SELECT 
    season, team, g, ab, r, h, hr, rbi, sb, obp, slg,
    (r / NULLIF(g, 0)) AS runs_per_game,
    COALESCE(obp, 0.000) AS obp_clean
FROM raw.pybaseball_team_batting;
"""

CREATE_PROCESSED_TEAM_PITCHING = """
CREATE OR REPLACE TABLE processed.pybaseball_team_pitching AS
SELECT 
    season, team, w, l, era, ip, so, whip, fip,
    (w * 1.0 / NULLIF((w + l), 0)) AS win_pct,
    COALESCE(era, 0.00) AS era_clean
FROM raw.pybaseball_team_pitching;
"""

CREATE_PROCESSED_PLAYER_BATTING = """
CREATE OR REPLACE TABLE processed.pybaseball_player_batting AS
SELECT 
    season, player_id, player_name, team, idfg, age, g, pa, ab, r, h, double, triple, hr, rbi, sb, cs, bb, so, hbp,
    COALESCE(avg, 0.000) AS avg_clean,
    COALESCE(obp, 0.000) AS obp_clean,
    COALESCE(slg, 0.000) AS slg_clean,
    COALESCE(ops, obp_clean + slg_clean) AS ops_clean,
    (pa / NULLIF(g, 0)) AS pa_per_game
FROM raw.pybaseball_player_batting;
"""

CREATE_PROCESSED_PLAYER_PITCHING = """
CREATE OR REPLACE TABLE processed.pybaseball_player_pitching AS
SELECT 
    season, player_id, player_name, team, idfg, age, w, l, era, g, gs, ip, h, r, er, bb, so, whip, era_plus,
    COALESCE(era, 0.00) AS era_clean,
    (w * 1.0 / NULLIF(w + l, 0)) AS win_pct,
    (so / NULLIF(ip, 0)) AS k_per_inning
FROM raw.pybaseball_player_pitching;
"""