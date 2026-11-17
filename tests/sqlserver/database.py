"""Explicit provisioning helpers for the SQL Server integration tests."""

import os

from dispatcherd.brokers.sqlserver import Broker, _rows, connect


def database_config(database="dispatcherd"):
    def escape(value):
        return "{" + value.replace("}", "}}") + "}"

    password = os.environ["MSSQL_SA_PASSWORD"]
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "14333")
    return {
        "connection_string": f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host},{port};"
        f"DATABASE={escape(database)};UID=sa;PWD={escape(password)};Encrypt=yes;TrustServerCertificate=yes;"
    }


def provision():
    connection = connect(database_config("master"))
    try:
        _rows(connection, "IF DB_ID('dispatcherd') IS NULL CREATE DATABASE dispatcherd")
    finally:
        connection.close()
    Broker.setup_database(database_config())


if __name__ == "__main__":
    provision()
    print("dispatcherd database provisioned")
