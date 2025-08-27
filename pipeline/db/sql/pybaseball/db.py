# pipeline/db/sql/pybaseball/db.py

CREATE_SCHEMAS = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS processed;
    CREATE SCHEMA IF NOT EXISTS features;
"""
