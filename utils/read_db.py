from sqlalchemy import create_engine, MetaData, Table, select

# Path to your airflow.db
db_path = "/home/vscode/airflow_poc/airflow.db"   # Update this path
engine = create_engine(f"sqlite:///{db_path}")

# Reflect the metadata
metadata = MetaData()
metadata.reflect(bind=engine)

# Get the `connection` table
connection_table = metadata.tables['connection']

# Query the table
with engine.connect() as conn:
    query = select([
        connection_table.c.conn_id,
        connection_table.c.conn_type,
        connection_table.c.host,
        connection_table.c.login,
        connection_table.c.password,
        connection_table.c.extra
    ])
    results = conn.execute(query)
    for row in results:
        print(dict(row))
