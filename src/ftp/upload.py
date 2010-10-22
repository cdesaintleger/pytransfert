# -*- coding: utf8 -*-
from ftplib import FTP, error_perm, all_errors
from Debug import Debug
import ConfigParser
from time import sleep


#lecture du fichier de config
conf    =   ConfigParser.ConfigParser()
conf.read("params.ini")

#mod debug
dbg = Debug.Debug('MyFTP')

def send_file(sem,file,destfile):

    try:
        #ouverture du fichier data
        f = open(file,'rb')
    except IOError:
        #erreur à l'ouverture du fichier
        dbg.print_err('sendFile','error opening [%s]' % file)
        #libere le semaphore
        sem.release()
        #quit la fonction
        return 1


    try:
        ftp =   FTP( conf.get("FTP", "HOST") )
        ftp.login( conf.get("FTP", "USER"), conf.get("FTP", "PASSWORD"))
        #ftp.mkd(destfile)
        #ftp.cwd(destfile)
        ftp.storbinary('STOR %s' %file, f)

    except error_perm, resp:

        dbg.print_err('Erreur : ', resp)
        sem.release()
        return 1
    
    except all_errors, resp:
        dbg.print_err('Erreur : ', resp)
        sem.release()
        return 1

    f.close()
    ftp.quit()
    sleep(30)
    sem.release()
    


