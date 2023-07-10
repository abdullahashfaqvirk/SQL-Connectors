"""
This module provides 'SQLServerDatabaseManager' class for interacting with a 'SQL Server' database.
Make sure to install the 'pyodbc' package before using this module.
pip install pyodbc
"""
import pyodbc


class SQLServerDatabaseManager:

    def __init__(self, server: str, database: str, username: str, password: str):
        self.__connection = None
        self.__cursor = None
        self.__connect(server, database, username, password)

    def __connect(self, server: str, database: str, username: str, password: str):
        """
        Establishes a connection to the SQL Server database.
        """
        connection_string = (
            f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password}"
        )
        self.__connection = pyodbc.connect(connection_string)
        self.__cursor = self.__connection.cursor()

    def insertData(self, tableName: str, tableAttributes: tuple, data: list[tuple]):
        """
        Inserts data into a specified table in the database.
        """
        query = f"INSERT INTO [{tableName}] {tableAttributes} VALUES ?"
        try:
            self.__cursor.executemany(query, data)
            self.__connection.commit()
        except pyodbc.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def fetchAllData(self, tableName: str, printRecords: bool = False):
        """
        Fetches all the data from a specified table in the database.
        """
        query = f"SELECT * FROM [{tableName}]"
        self.__cursor.execute(query)
        data = self.__cursor.fetchall()
        if printRecords:
            for record in data:
                print(record)
        return data

    def updateData(self,
                   tableName: str,
                   tableAttribute: str,
                   updatedData: int | str,
                   id_column: str,
                   id_value: str
                   ):
        """
        Updates data in a specified table and attribute based on a given ID.
        """
        query = f"UPDATE [{tableName}] SET [{tableAttribute}] = ? WHERE [{id_column}] = ?"
        parameters = (updatedData, id_value)

        try:
            self.__cursor.execute(query, parameters)
            self.__connection.commit()
        except pyodbc.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def deleteData(self, tableName: str, tableAttribute: str, id: str):
        """
        Deletes data from a specified table based on a given ID.
        """
        query = f"DELETE FROM [{tableName}] WHERE [{tableAttribute}] = ?"
        try:
            self.__cursor.execute(query, id)
            self.__connection.commit()
        except pyodbc.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def deleteOrTruncateTable(self, tableName: str, operation: str):
        """
        Deletes or truncates a table in the database.

        Args:
            tableName (str): The name of the table to be deleted or truncated.
            operation (str): The operation to perform. Should be either 'delete' or 'truncate'.
        """
        if operation == 'delete':
            query = f"DROP TABLE [{tableName}]"
            message = f"Table '{tableName}' deleted successfully."
        elif operation == 'truncate':
            query = f"TRUNCATE TABLE [{tableName}]"
            message = f"Table '{tableName}' truncated successfully."
        else:
            print(
                "Error: Invalid operation. Please provide either 'delete' or 'truncate'."
            )
            return

        try:
            self.__cursor.execute(query)
            self.__connection.commit()
            print(message)
        except pyodbc.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def closeConnection(self):
        """
        Closes the connection and cursor to the database.
        """
        self.__cursor.close()
        self.__connection.close()
