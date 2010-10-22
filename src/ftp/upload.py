# -*- coding: utf8 -*-
from threading import Thread
from ftplib import FTP, error_perm, all_errors
from Debug import Debug
import ConfigParser
from time import sleep


class MyFtp(Thread):

    def __init__(self,sem,file,destfile):

        #initialisation du thread
        Thread.__init__(self)

        #lecture du fichier de config
        self.conf    =   ConfigParser.ConfigParser()
        self.conf.read("params.ini")

        #mod debug
        self.dbg = Debug.Debug('MyFTP')

        #recupération de jetons ( semaphore )
        self.sem = sem

        #recupre le fichier à uploader
        self.file   =   file
        #recupére la destination 
        self.destfile   =   destfile


    def run(self):

        print "Attente du thread => ", self.file
        self.sem.acquire()
        try:
            print "Execution du thread => ", self.file
            self._send_file(self.file, self.destfile)
        finally:
            print "Fin du thread => ", self.file
            self.sem.release()



    def _send_file(self, file, destfile):

        try:
            #ouverture du fichier data
            f = open(file,'rb')
        except IOError:
            #erreur à l'ouverture du fichier
            self.dbg.print_err('sendFile','error opening [%s]' % file)
            #quit la fonction
            return 1


        try:
            ftp =   FTP( self.conf.get("FTP", "HOST") )
            ftp.login( self.conf.get("FTP", "USER"), self.conf.get("FTP", "PASSWORD"))
            try:
                ftp.mkd(destfile)
            except error_perm, resp:
                self.dbg.print_err("Repertoire deja existant .. on passe ", resp)
            finally:
                ftp.cwd(destfile)
                
            ftp.storbinary('STOR %s' %file, f)

        except error_perm, resp:

            self.dbg.print_err('Erreur : ', resp)
            return 1

        except all_errors, resp:
            self.dbg.print_err('Erreur : ', resp)
            return 1

        f.close()
        ftp.quit()
        sleep(15)
    


