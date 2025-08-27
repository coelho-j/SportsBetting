# FEATURES LAYER: Derived metrics
CREATE_FEATURES_TEAM_FEATURES = """
CREATE OR REPLACE TABLE features.team_features AS
SELECT 
    b.season, b.team,
    b.runs_per_game AS offensive_rating,
    p.win_pct * 100 AS pitching_strength,
    (b.obp_clean + (p.era_clean / 10)) AS combined_metric
FROM processed.pybaseball_team_batting b
JOIN processed.pybaseball_team_pitching p ON b.team = p.team AND b.season = p.season;
"""

CREATE_FEATURES_PLAYER_FEATURES = """
CREATE OR REPLACE TABLE features.player_features AS
SELECT 
    pb.season, pb.player_id, pb.player_name,
    pb.pa_per_game AS plate_appearances_efficiency,
    (pb.hr / NULLIF(pb.ab, 0)) * 100 AS hr_rate,
    pp.win_pct * 100 AS pitching_win_strength,
    (pb.ops_clean + (pp.era_clean / 10)) AS batter_pitcher_adjusted_metric
FROM processed.pybaseball_player_batting pb
LEFT JOIN processed.pybaseball_player_pitching pp ON pb.player_id = pp.player_id AND pb.season = pp.season;
"""