import mysql.connector
from typing import List, Tuple


def get_tables(conn_params) -> List[Tuple[str, str]]:
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor()
        cur.execute(
            """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """,
            (conn_params["database"],),
        )
        return cur.fetchall()
    finally:
        cnx.close()


def get_primary_key_columns(conn_params, schema: str, table: str) -> List[str]:
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor()
        cur.execute(
            """
            SELECT k.COLUMN_NAME
            FROM information_schema.table_constraints t
            JOIN information_schema.key_column_usage k
              ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
             AND t.TABLE_SCHEMA = k.TABLE_SCHEMA
             AND t.TABLE_NAME = k.TABLE_NAME
            WHERE t.TABLE_SCHEMA = %s
              AND t.TABLE_NAME = %s
              AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY k.ORDINAL_POSITION
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cnx.close()
