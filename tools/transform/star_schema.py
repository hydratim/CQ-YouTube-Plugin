from db import *


def sync():
    for table in DestinationTables.get_tables():
        table.sync(SourceTables)


if __name__ == "__main__":
    source_db.bind(provider="sqlite", filename="../../db.sqlite")
    destination_db.bind(
        provider="postgres",
        user="postgres",
        password="postgrespw",
        host="127.0.0.1",
        port=32768,
        database="analytics",
    )
    source_db.generate_mapping(create_tables=False)
    destination_db.generate_mapping(create_tables=True)
    with orm.db_session():
        sync()
