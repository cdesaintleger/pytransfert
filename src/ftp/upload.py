# -*- coding: utf8 -*-
from threading import Thread
from ftplib import FTP, error_perm, all_errors
from Debug import Debug
import ConfigParser
from time import sleep
from bdd import acces_bd


class MyFtp(Thread):

    def __init__(self,sem,file):

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

        #Connexion SQL pour la changement des etats
        #instanciation à la base
        self.sql  =   acces_bd.Sql()

        #Paramétres de connection
        self.sql.set_db(self.conf.get("DDB", "DATABASE"))
        self.sql.set_host(self.conf.get("DDB", "HOST"))
        self.sql.set_user(self.conf.get("DDB", "USER"))
        self.sql.set_password(self.conf.get("DDB", "PASSWORD"))
        #connection effective
        self.sql.conn()      

    #action du thread ( start )
    def run(self):

        #Signale que l'on met en file le fichier
        print "Attente du thread => ", self.file[1]
        #aquisition d'un jeton ( semaphore ) ou attente d'une libération
        self.sem.acquire()

        #Changement d'état en base => 2 upload en cours
        self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 2 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

        try:
            #jeton acquis , signalement du lancement de l'upload du fichier
            print "Execution du thread => ", self.file[1]
            #envoie du fichier au module FTP
            self._send_file()

        finally:
            #signalement de la fin de l'upload donc du thread
            print "Fin du thread => ", self.file[1]

            #Changement d'état en base => 3 upload terminé
            self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 3 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

            #libération du jeton pour laisser la place à un autre
            self.sem.release()



    #Méthode _send_file gére les transactions avec le serveur FTP 
    def _send_file(self):

        try:
            #ouverture du fichier data
            f = open(self.file[3]+self.file[1],'rb')
        except IOError:
            #erreur à l'ouverture du fichier
            self.dbg.print_err('sendFile','error opening [%s]' % self.file[1])

            #Changement d'état en base => 404 Fichier introuvable
            self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 404 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

            #quit la fonction
            return 1


        try:
            #connection au serveur FTP
            ftp =   FTP( self.conf.get("FTP", "HOST") )
            #Login avec user <-> password
            ftp.login( self.conf.get("FTP", "USER"), self.conf.get("FTP", "PASSWORD"))
            try:
                #creation du repertoire destination
                ftp.mkd(self.file[4])
            except error_perm, resp:
                #si le repertoire existe déjà .. on signale et on passe
                self.dbg.print_err("Repertoire deja existant .. on passe ", resp)
            finally:
                #on se déplace dans le repertoire finale
                ftp.cwd(self.file[4])

            #Lancement de l'upload proprement dit#
            ftp.storbinary('STOR %s' %self.file[1], f)

        except error_perm, resp:

            #Changement d'état en base => 500 Probleme de connection ou d'ecriture
            self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 500 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

            self.dbg.print_err('Erreur : ', resp)
            return 1

        except all_errors, resp:

            #Changement d'état en base => 500 Probleme de connection ou d'ecriture
            self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 500 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

            self.dbg.print_err('Erreur : ', resp)
            return 1

        #fermeture du fichier
        f.close()
        #cloture de la connection FTP
        ftp.quit()

        #debug
        #sleep(15)
    


