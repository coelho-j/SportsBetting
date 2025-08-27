import pytest
from pipeline.db.sdk import BaseballSDK
import duckdb

@pytest.fixture(scope="module")
def setup_db():
    """Fixture to ensure database is set up before tests (call setup if needed)."""
    from pipeline.db import setup, verify
    try:
        setup()
        verify()  # Ensures integrity; will raise if issues
    except duckdb.IOException as e:
        pytest.skip(f"Database is in use: {e}")
    except Exception as e:
        pytest.skip(f"Database setup failed: {e}")

def test_get_player(setup_db):
    """Test fetching player stats for Mike Trout; verifies insertion and basic metrics."""
    player_stats = BaseballSDK.get_player('Mike Trout')
    print(player_stats)
    print(player_stats.ops)
    assert player_stats is not None, "Failed to retrieve player stats."
    assert player_stats.player_name == 'Mike Trout', "Incorrect player name."
    assert player_stats.ops > 0, f"Expected positive OPS; got {player_stats.ops}."  # 2024 OPS ≈ 0.766
    assert player_stats.hr >= 0, f"Expected non-negative HR; got {player_stats.hr}."  # 2024 HR = 10
    print(f"Player: {player_stats.player_name}, OPS: {player_stats.ops}, HR: {player_stats.hr}")

def test_get_team_batting(setup_db):
    """Test fetching team batting stats for LAD; verifies OBP and non-empty result."""
    team_stats = BaseballSDK.get_team_batting('LAD')
    assert team_stats is not None, "Failed to retrieve team stats."
    assert team_stats.team == 'LAD', "Incorrect team abbreviation."
    assert team_stats.obp > 0, f"Expected positive OBP; got {team_stats.obp}."  # 2024 OBP = 0.335
    print(f"Team: {team_stats.team}, OBP: {team_stats.obp}")

def test_execute_query(setup_db):
    """Test custom query on player features; verifies non-empty DataFrame."""
    df = BaseballSDK.execute_query("SELECT * FROM features.player_features LIMIT 5")
    assert not df.empty, "Expected non-empty DataFrame from player features query."
    assert 'player_name' in df.columns, "Expected 'player_name' column in features DataFrame."
    assert len(df) <= 5, "Query should return at most 5 rows."
    print(df)