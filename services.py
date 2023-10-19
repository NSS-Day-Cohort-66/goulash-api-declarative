import sqlite3


def expand_hauler(row):
    dock = {
        "id": row["dockId"],
        "location": row["location"],
        "capacity": row["capacity"],
    }
    hauler = {
        "id": row["id"],
        "name": row["name"],
        "dock_id": row["dock_id"],
        "dock": dock,
    }

    return hauler
