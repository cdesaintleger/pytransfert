# -*- coding: utf8 -*-
from threading import Thread, BoundedSemaphore
import ConfigParser
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
        
        

class Transfert(Thread):
    
        
    def __init__(self):

        #lecture du fichier de config
        self.conf    =   ConfigParser.ConfigParser()
        self.conf.read("params.ini")

        #définition du sémaphore ( nb upload simu. )
        self.sem =   BoundedSemaphore(self.conf.getint("GLOBAL","NBTHREAD"))
        
        
        
    def upload_ftp(self,list,logger,conf,sql):
        
        #tableau des threads
        ThUp    =   []
        
        logger.info("%s -- INFO -- connection au FTP "% (strftime('%c',localtime())) )
        #Une seule connexion FTP par session 
        ftp =   ftputil.FTPHost( self.conf.get("FTP", "HOST"), self.conf.get("FTP", "USER"), self.conf.get("FTP", "PASSWORD"), self.conf.getint("FTP", "PORT"), session_factory=MySession)
        
        #parcour des fichiers
        for file in list:
            
            #upload du fichier par un thread
            mup =   upload.MyFtp(ftp,self.sem,file,logger,conf,sql)
            mup.start()
            ThUp.append( mup )
            
            
        #Tant qu'un thread est actif
        while len( ThUp ) > 0:
            
            
            logger.info("%s -- INFO -- Threads en cours :  -- %s"% (strftime('%c',localtime()),str(len( ThUp ))) )
            
            for (key,th) in enumerate(ThUp):
                if not th.isAlive():
                    del(ThUp[key])
                    
            time.sleep(1)
                    
        logger.info("%s -- INFO -- Deconnection du FTP -- "% (strftime('%c',localtime())) )
        ftp.close()

           
