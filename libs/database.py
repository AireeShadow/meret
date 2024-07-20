import json
import sqlite3 as sql
import os
import shutil

from libs.logger import Logger


#
#Module for all classes, related to db operations
#
class Basic_Db_Operations(Logger):
    '''
    Params:
        location: str - path to the DB file location (must be a directory!)
    Creates database and tables, backups DB, deletes DB
    '''

    def __init__(self, location: str) -> None:
        super().__init__()
        self.db_location = f'{location}/meret.db'
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
        #Label_subgroup should be a JSON
        group_table = '''
            CREATE TABLE Groups (
            Label_group TEXT NOT NULL,
            Label_subgroups TEXT
        );
        ''' 
        #Labels should be a JSON
        subroup_table = '''
            CREATE TABLE Subgroup (
            Label_subgroup TEXT NOT NULL,
            Labels TEXT,
            Colour TEXT
        );
        '''
        #Files should be a JSON
        labels_table = '''
            CREATE TABLE Labels (
            Label TEXT NOT NULL,
            Files TEXT
        );
        '''
        return [
            filename_table,
            group_table,
            subroup_table,
            labels_table
        ]
        

    def create_tables(self) -> bool:
        '''
        Creates initial tables
        '''

        if not os.path.exists(self.db_location):
            self.logger.info(f'Creating database "meret.db" in the location {self.db_location}')
            with sql.connect(self.db_location) as connect_obj:
                cursor_obj = connect_obj.cursor()
                tables_list = Basic_Db_Operations._table_list()
                for table_sql in tables_list:
                    cursor_obj.execute(table_sql)
            return True
        else:
            self.logger.warning(f'Not creating tables, database file exists at {self.db_location}')
            return False

    def backup_db(self) -> bool:
        '''
        Creates a backup for DB file
        Returns: True if success, False if not
        '''

        self.logger.info('Creating a backup for thr DB')
        try:
            shutil.copyfile(self.db_location, f'{self.db_location}.bak')
        except Exception as ex:
            self.logger.warn(f'An exception occurs during the backuping: {ex}')
            return False
        else:
            self.logger.info('Backup is completed!')
            return True
    
    def restore_db(self) -> bool:
        '''
        Restores a DB file from backup
        Returns: True if success, False if not
        '''

        self.logger.info('Restoring a backup for the DB')
        try:
            shutil.copyfile(f'{self.db_location}.bak', self.db_location)
        except Exception as ex:
            self.logger.warn(f'An exception occurs during the restoring: {ex}')
            return False
        else:
            self.logger.info('Restore is completed!')
            return True
        
    def delete_db(self) -> bool:
        '''
        Permanently deletes the DB
        Returns: True if success, False if not
        '''
        
        if os.path.exists(self.db_location):
            self.logger.info('Deleting the db...')
            try:
                os.remove(self.db_location)
            except Exception as ex:
                self.logger.warn(f'An exception occurrs during the removal: {ex}')
                return False
            else:
                self.logger.info('Removal is completed!')
                return True
        else:
            self.logger.warn('DB is not present, not deleting anything')