import sqlite3 as sql
import json
import sqlite3 as sql
import os
import shutil
from libs.logger import Logger


class DB_Operations(Logger):
   
    '''
    Params:
        location: str - path to the DB file location (must be a directory!)
    Creates tables, Select Column on table, Insert Column on table, Update Column on table, 
    Delete Column on table
    '''
       
    def __init__(self, location: str) -> None:
        super().__init__()
        self.db_location = f'{location}/test.db'
            # self.db_path = Path(self.db_location)
        if not isinstance(self.db_location, str):
            self.logger.critical(f'DB location should be str type, not {type(self.db_location)}!')
            raise ValueError
        
        
    @staticmethod
    def _table_list() -> list[str]:

        '''
        Returns: a list of SQL statements as strings
        '''
        #Labels and EXIF are meant to be stored as JSON and then loaded if needed
        filename_table = '''
            CREATE TABLE Files (
            filename TEXT NOT NULL,
            labels TEXT,
            rating INT,
            type TEXT,
            label_group TEXT,
            label_subgroup TEXT,
            EXIF TEXT
        ); 
        '''
      
        return [
            filename_table
        ]

        

    def create_tables(self) -> bool:
        '''
        Creates initial tables
        '''

        if not os.path.exists(self.db_location):
            self.logger.info(f'Creating database "test.db" in the location {self.db_location}')
            with sql.connect(self.db_location) as connect_obj:
                cursor_obj = connect_obj.cursor()
                tables_list = DB_Operations._table_list()
                for table_sql in tables_list:
                    cursor_obj.execute(table_sql)
            return True
        else:
            self.logger.warning(f'Not creating tables, database file exists at {self.db_location}')
            return False