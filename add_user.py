#!/usr/bin/env python3
"""Add or update a user in users.json. Run from the repo root."""
import bcrypt
import json
import sys
from pathlib import Path

USERS_FILE = Path("users.json")


def add_user(username: str, password: str) -> None:
    users = json.loads(USERS_FILE.read_text()) if USERS_FILE.exists() else {}
    action = "Updated" if username in users else "Added"
    users[username] = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    USERS_FILE.write_text(json.dumps(users, indent=2))
    print(f"{action} user '{username}'.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python add_user.py <username> <password>")
        sys.exit(1)
    add_user(sys.argv[1], sys.argv[2])
