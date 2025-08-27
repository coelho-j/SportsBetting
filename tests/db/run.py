# tests/db/run.py

import pipeline.db as db
import pytest
import sys
import os

def main():
    print("Verifying db")
    try:
        db.verify()
    except Exception:
        print("Attempting setup on db")
        db.setup()
        try:
            db.verify()
        except Exception as e:
            print(f"Error verifying database: {e}")
            sys.exit(1)
        print("Setup successfully solved the problem.")

    print("Running tests!")
    test_sdk_path = os.path.join(os.path.dirname(__file__), 'test_sdk.py')
    sys.exit(pytest.main([test_sdk_path]))

if __name__ == "__main__":
    main()