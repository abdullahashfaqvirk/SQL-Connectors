"""
This module provides 'MySQLDatabaseManager' class for interacting with a MySQL database.
Make sure to install the 'mysql.connector' package before using this module.
pip install mysql-connector-python
"""
import mysql.connector


class MySQLDatabaseManager:

    def __init__(self, host: str, database: str, username: str, password: str):
        self.__connection = None
        self.__cursor = None
        self.__connect(host, database, username, password)

    def __connect(self, host: str, database: str, username: str, password: str):
        """
        Establishes a connection to the MySQL database.
        """
        connection_params = {
            'host': host,
            'database': database,
            'user': username,
            'password': password
        }
        self.__connection = mysql.connector.connect(**connection_params)
        self.__cursor = self.__connection.cursor()

    def insertData(self, tableName: str, tableAttributes: str, data: list[tuple]):
        """
        Inserts data into a specified table in the database.
        """
        placeholders = ', '.join(['%s'] * len(tableAttributes.split(', ')))
        query = f"INSERT INTO `{tableName}` {tableAttributes} VALUES ({placeholders})"
        try:
            self.__cursor.executemany(query, data)
            self.__connection.commit()
        except mysql.connector.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def fetchAllData(self, tableName: str, printRecords: bool = False):
        """
        Fetches all the data from a specified table in the database.
        """
        query = f"SELECT * FROM `{tableName}`"
        try:
            self.__cursor.execute(query)
            data = self.__cursor.fetchall()
            if printRecords:
                for record in data:
                    print(record)
            return data
        except mysql.connector.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def updateData(self,
                   tableName: str,
                   tableAttribute: str,
                   updatedData: int | str,
                   id_column: str,
                   id_value: str):
        """
        Updates data in a specified table and attribute based on a given ID.
        """
        query = f"UPDATE `{tableName}` SET `{tableAttribute}` = %s WHERE `{id_column}` = %s"
        parameters = (updatedData, id_value)

        try:
            self.__cursor.execute(query, parameters)
            self.__connection.commit()
        except mysql.connector.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def deleteData(self, tableName: str, tableAttribute: str, id_value: str):
        """
        Deletes data from a specified table based on a given ID.
        """
        query = f"DELETE FROM `{tableName}` WHERE `{tableAttribute}` = %s"
        try:
            self.__cursor.execute(query, (id_value,))
            self.__connection.commit()
        except mysql.connector.Error as e:
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
            query = f"DROP TABLE `{tableName}`"
            message = f"Table '{tableName}' deleted successfully."
        elif operation == 'truncate':
            query = f"TRUNCATE TABLE `{tableName}`"
            message = f"Table '{tableName}' truncated successfully."
        else:
            print(
                "Error: Invalid operation. Please provide either 'delete' or 'truncate'.")
            return

        try:
            self.__cursor.execute(query)
            self.__connection.commit()
            print(message)
        except mysql.connector.Error as e:
            self.__connection.rollback()
            print(f"Error: {str(e)}")

    def closeConnection(self):
        """
        Closes the connection and cursor to the database.
        """
        self.__cursor.close()
        self.__connection.close()
