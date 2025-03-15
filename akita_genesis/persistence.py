# akita_genesis/persistence.py
import sqlite3
import json

class DataStore:
    def __init__(self, db_name="akita.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                entry TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS state (
                data TEXT
            )
        """)
        self.conn.commit()

    def save_ledger(self, entry):
        self.cursor.execute("INSERT INTO ledger (entry) VALUES (?)", (json.dumps(entry),))
        self.conn.commit()

    def load_ledger(self):
        self.cursor.execute("SELECT entry FROM ledger")
        return [json.loads(row[0]) for row in self.cursor.fetchall()]

    def save_state(self, state_data):
        self.cursor.execute("DELETE FROM state")
        self.cursor.execute("INSERT INTO state (data) VALUES (?)", (json.dumps(state_data),))
        self.conn.commit()

    def load_state(self):
        self.cursor.execute("SELECT data FROM state")
        result = self.cursor.fetchone()
        return json.loads(result[0]) if result else {}
