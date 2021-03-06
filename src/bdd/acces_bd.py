# -*- coding: utf8 -*-

import MySQLdb
from time import strftime, localtime

class Sql:

    #Paramétre de connection à la db
    __DB  =   ''
    __DB_HOST = ''
    __DB_USER = ''
    __DB_PASSWORD = ''
    __DB_ENGINE = ''
    

    #ressource vers la ddb
    conn    =   ''


    #Constructeur connection à la base
    def __init__(self,logger):
        self.logger =   logger
    #    return none

    #destructeur fermeture de la connection
    def __del__(self):
        self.logger.info("%s -- DEBUG -- Fermeture connexion SQL ...  -- "% (strftime('%c',localtime())) )
        self.conn.close()


    #
    #   Les Accesseurs
    #

    def set_db(self,value):
        self.__DB   =   value

    def set_host(self,value):
        self.__DB_HOST  =   value

    def set_user(self,value):
        self.__DB_USER  =   value

    def set_password(self,value):
        self.__DB_PASSWORD  =   value
        
    def set_db_engine(self,value):
        self.__DB_ENGINE    =   value

    
    def conn(self):
        self.conn = MySQLdb.Connection(db=self.__DB, host=self.__DB_HOST, user=self.__DB_USER,passwd=self.__DB_PASSWORD)


    #execution de la requete passé en paramétre
    def execute(self,sql,type="select"):

        
        cursor = self.conn.cursor()
        res = cursor.execute(sql)

        if( self.__DB_ENGINE == "innodb" ):
            self.conn.commit()

        if( type == "select" ):
            res    =   cursor.fetchall()

        cursor.close()

        return res
