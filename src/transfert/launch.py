# -*- coding: utf8 -*-
from threading import Thread

from ftp import upload
import time


#ftp
import ftplib
from libftputil import ftputil

#logging
from time import strftime, localtime


class MySession(ftplib.FTP):
    
    def __init__(self, host, userid, password, port):
        """Act like ftplib.FTP's constructor but connect to another port."""
        ftplib.FTP.__init__(self)
        self.connect(host, port)
        self.login(userid, password)
        
   



class Worker(Thread):
    
        
    def __init__(self,logger,conf,sql,queue):
        
        #initialisation du thread
        Thread.__init__(self)
        
        #recupération du handle sur le fichier de config
        self.conf       =   conf
        #Recuperation du handle sur le gestionnaire de logs
        self.logger     =   logger
        
        #recupere le main sur la connexion sql
        self.sql    =   sql
        
        #Remonte la queue
        self.queue  =   queue
        
        
    def run(self):
        
        
        while True:
            
            #Attend un fichier à traiter
            file    =   self.queue.get()
            
            self.logger.info("%s -- INFO -- Recuperation d'un fichier a traiter "% (strftime('%c',localtime())) )
            
            self.logger.info("%s -- INFO -- connection au FTP "% (strftime('%c',localtime())) )
            
            #Une seule connexion FTP par session 
            ftp =   ftputil.FTPHost(
                self.conf.get("FTP", "HOST"),
                self.conf.get("FTP", "USER"),
                self.conf.get("FTP", "PASSWORD"),
                self.conf.getint("FTP", "PORT"),
                session_factory=MySession)
            
            
            mup =   upload.MyFtp(ftp,file,self.logger,self.conf,self.sql)
            mup.start()
            
            #Attente de la fin du transfert
            mup.join()
            
            #Fermeture de la connexion FTP
            ftp.close()
            del(ftp)
            
            self.logger.info("%s -- INFO -- de-connection du FTP "% (strftime('%c',localtime())) )
            
            #Notifi la file (queue) que le travail est terminé
            self.queue.task_done()