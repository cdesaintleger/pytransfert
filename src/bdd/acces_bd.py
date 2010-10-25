# -*- coding: utf8 -*-

import MySQLdb

class Sql:

    #Paramétre de connection à la db
    __DB  =   ''
    __DB_HOST = ''
    __DB_USER = ''
    __DB_PASSWORD = ''
    

    #ressource vers la ddb
    conn    =   ''


    #Constructeur connection à la base
    #def __init__(self):
    #    return none

    #destructeur fermeture de la connection
    def __del__(self):
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

    
    def conn(self):
        self.conn = MySQLdb.Connection(db=self.__DB, host=self.__DB_HOST, user=self.__DB_USER,passwd=self.__DB_PASSWORD)


    #execution de la requete passé en paramétre
    def execute(self,sql,type="select"):

        
        cursor = self.conn.cursor()
        cursor.execute(sql)

        if( type == "select" ):
            res    =   cursor.fetchall()
            cursor.close()
        else:
            res    =    null

        return res
