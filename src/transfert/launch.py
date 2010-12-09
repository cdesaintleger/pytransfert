# -*- coding: utf8 -*-
from threading import Thread, BoundedSemaphore
import ConfigParser
from ftp import upload


class Transfert(Thread):
    
        
    def __init__(self):

        #lecture du fichier de config
        self.conf    =   ConfigParser.ConfigParser()
        self.conf.read("params.ini")

        #définition du sémaphore ( nb upload simu. )
        self.sem =   BoundedSemaphore(self.conf.getint("GLOBAL","NBTHREAD"))

        #tableau des threads
        self.ThUp    =   {}

        
    def upload_ftp(self,list,logger,conf):

        
        #parcour des fichiers
        for file in list:
            
            #upload du fichier par un thread
            upload.MyFtp(self.sem,file,logger,conf).start()

           
