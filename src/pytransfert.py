#!/usr/bin/python
# -*- coding: utf8 -*-

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="christophe de saint leger"
__date__ ="$05 Juin 2011 17:51:23$"

#importation des lib necessaires
from bdd import acces_bd
import rq

from transfert import launch
import threading
import ConfigParser
import os,sys
import warnings

#logging
from time import strftime, localtime, sleep

#logging
import logging
import logging.handlers

#Gestionnaire de la file
from Queue import *
queue = Queue()

  
#############################################
##                                         ##
##                  MAIN                   ##
##                                         ##
#############################################

class MainPytransfert(threading.Thread):

    def __init__(self,tempo, sql, conf, logger):
        
        self.tempo  =   tempo
        self.sql  =   sql
        self.conf   =   conf
        self.logger =   logger
        threading.Thread.__init__(self);

    def run(self):
        self.maintimer(self.tempo, self.sql, self.conf, self.logger)

    def maintimer(self, tempo, sql, conf, logger):

        while True:

            self.logger.info("%s -- DEBUG -- Reveil du thread MainPytransfert ...  -- "% (strftime('%c',localtime())) )

            res = rq.get_new_files(sql,conf)

            #compte le nombre de resultats trouvés
            nbFiles =   len(res)

            #log du nombre de fichiers à traiter
            logger.info("%s -- INFO -- Fichiers à traiter : %s -- "% (strftime('%c',localtime()),str(nbFiles) ) )

            #lancement uniquement sil y a des fichiers à uploader
            if( nbFiles > 0 ):
                
                
                #parcour des fichiers
                listeid =  list()

                #remonte le id
                for file in res:
                    listeid.append(str(file[0]))
                
                logger.info("-- SQL -- Mise en file des enregistrements :  "+','.join(listeid))

                #On marque tout ces fichiers comme "En file" etat = 1
                rq.set_state(sql,conf,listeid,1)
                
                #Met les fichiers en file
                for file in res:
                    queue.put(file)
                    listeid.append(str(file[0]))             
                
            self.logger.info("%s -- DEBUG -- Mise en pause du thread MainPytransfert ...  -- "% (strftime('%c',localtime())) )

            #Pause
            sleep(tempo)





class MainCleaner(threading.Thread):


    def __init__(self,tempo,sql,conf,logger):

        self.tempo  =   tempo
        self.conf   =   conf
        self.logger =   logger
        self.sql    =   sql
        
        #initialisation du thread
        threading.Thread.__init__(self)

    #action du thread ( start )
    def run(self):
        self.cleaner_timer(self.tempo, self.sql, self.conf, self.logger)


    #Méthode de nettoyage des fichiers uploadés
    def cleaner_timer(self, tempo, sql, conf, logger):

        while True:

            self.logger.info("%s -- DEBUG -- Reveil du thread MainCleaner ...  -- "% (strftime('%c',localtime())) )

            
            #Recupére les images à transferer ( nouvelles + échouées )
            res =   sql.execute("\
                SELECT \
                "+str(conf.get("DDB","CHAMP_ID"))+",\
                "+str(conf.get("DDB","CHAMP_IMG"))+",\
                "+str(conf.get("DDB","CHAMP_SOURCE"))+"\
                FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
                WHERE ( "+str(conf.get("DDB","CHAMP_ETAT"))+" in (3,-3)\
                AND TO_DAYS( NOW() ) - TO_DAYS("+str(conf.get("DDB","CHAMP_DATE"))+") > "+str(conf.get("GLOBAL","JOURS_RETENTION"))+")" )
            
            
            
            
            
            #Récupére les fichiers panier > 3jours
            res_panier  =   sql.execute("\
                SELECT \
                "+str(conf.get("DDB","CHAMP_ID"))+",\
                "+str(conf.get("DDB","CHAMP_IMG"))+",\
                "+str(conf.get("DDB","CHAMP_SOURCE"))+"\
                FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
                WHERE ( "+str(conf.get("DDB","CHAMP_ETAT"))+" in (-1)\
                AND TO_DAYS( NOW() ) - TO_DAYS("+str(conf.get("DDB","CHAMP_DATE"))+") > 3)" )
            
            
            listeid_to_delete =  list()
            if(len(res_panier) > 0):
                
                #Récupére les id des lignes à supprimer
                for old_file in res_panier:
                    
                    logger.info("%s -- INFO -- Réouverture upload de : %s / %s -- "% (strftime('%c',localtime()),str(old_file[2]),str(old_file[1])) )
                    listeid_to_delete.append(str(old_file[0]))
                
                #Fusion des resultats de requetes pour suppression physique des fichiers client
                res = res+res_panier
                
                
                
                
            

            if( len(res) > 0 ):
                for file in res:

                    logger.info("%s -- INFO -- Nettoyage de : %s / %s -- "% (strftime('%c',localtime()),str(file[2]),str(file[1])) )

                    try:
                        directory   =   str(file[2]).strip("/")
                        fichier     =   str(file[1]).strip("/")

                        os.remove("/"+directory+"/"+fichier)
                        #On marque tout ces fichiers comme "nettoyé"
                        sql.execute("UPDATE "+str(conf.get("DDB","TBL_ETAT"))+"\
                        SET "+str(conf.get("DDB","CHAMP_ETAT"))+" = 33 \
                        WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+str(file[0])+")")
                    except:
                        #marque le fichier comme impossible à nettoyer
                        sql.execute("UPDATE "+str(conf.get("DDB","TBL_ETAT"))+"\
                        SET "+str(conf.get("DDB","CHAMP_ETAT"))+" = 304 \
                        WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+str(file[0])+")")
                        warnings.warn("Impossible de supprimer un fichier !\n")
                        
                        
                        
                        
            #Ouverture des espace d'upload pour le panier agé de plus de 3 jours            
            if( len(res_panier)>0 ):
                
                #Suppression des enregistrements en base ( paniers expirés )
                sql.execute("\
                            DELETE FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
                            WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+','.join(listeid_to_delete)+")\
                ")
            

            self.logger.info("%s -- DEBUG -- Mise en pause du thread MainCleaner ...  -- "% (strftime('%c',localtime())) )

            #Pause
            sleep(tempo)








#############################################
##                                         ##
##      Lancement De la boucle infinie     ##
##                                         ##
#############################################
gl_rotation_ftp     =   0
gl_rotation_clean   =   0

if __name__ == "__main__":

    #Détache le process fils
    pid = os.fork()
    if pid:
        
        print ">>> Le Pére: Fils "+str(pid)+" ou es tu ? ... je te quitte nous nous retrouverons au prochain reboot ..."
        
        # Parent, write PID file
        fp = open('pytransfert.pid', 'w')
        fp.write(str(pid))
        fp.flush()

        # Forcibly sync disk
        os.fsync(fp.fileno())
        fp.close()

        os._exit(0)
        
        sys.exit(os.EX_OK)

    else:
        
        print ">>> Le Fils ( pid: "+str(os.getpid())+" ): Pére je suis là ... je vais méner la mission à bien ne t'en fait pas ... mon créaeur est un génie .. ' oulà .. les chevilles :p  '"
        print ">>> Répertoire de travail : "+os.getcwd()

        #lecture du fichier de config
        conf    =   ConfigParser.ConfigParser()
        conf.read("params.ini")

        #mise en place du logger
        LOG_FILENAME = 'log/pytransfert.out'

        # Set up a specific logger with our desired output level
        logger = logging.getLogger('pyTransfert')
        logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=16777216, backupCount=5)

        logger.addHandler(handler)




        #instanciation à la base
        sql  =   acces_bd.Sql(logger)

        #Paramétres de connection
        sql.set_db(conf.get("DDB", "DATABASE"))
        sql.set_host(conf.get("DDB", "HOST"))
        sql.set_user(conf.get("DDB", "USER"))
        sql.set_password(conf.get("DDB", "PASSWORD"))
        sql.set_db_engine(conf.get("DDB", "ENGINE"))
        #connection effective
        sql.conn()



        #Lancement des workers
        workers =   []
        for i in range(conf.getint("GLOBAL","NBTHREAD")):
            
            worker   =   launch.Worker(logger,conf,sql,queue)
            worker.start()
            workers.append(worker)
            
            
        #Lancement main qui s'occupe de remplir la file queue
        pyt_thread = MainPytransfert(conf.getint("GLOBAL", "TIMER"), sql, conf, logger)
        pyt_thread.start()

        #Gestion du nettoyae automatique
        cln_thread  =   MainCleaner(conf.getint("GLOBAL","CLEANER_TIMER"), sql, conf, logger)
        cln_thread.start()
        
        
        
        
        #Verification du bon fonctionnement des threads
        while True:

            if pyt_thread.isAlive() != True :

                logger.info("%s -- WARN -- Tread main stopé , restart ...  -- "% (strftime('%c',localtime())) )

                del pyt_thread
                pyt_thread = MainPytransfert(conf.getint("GLOBAL", "TIMER"), sql, conf, logger)
                pyt_thread.start()
                

            else:
                
                logger.info("%s -- DEBUG -- Tread main is alive ...  -- "% (strftime('%c',localtime())) )

            if cln_thread.is_alive() != True :

                logger.info("%s -- WARN -- Tread cleaner stopé , restart ...  -- "% (strftime('%c',localtime())) )

                del cln_thread
                cln_thread  =   MainCleaner(conf.getint("GLOBAL","CLEANER_TIMER"), sql, conf, logger)
                cln_thread.start()

            else:
                logger.info("%s -- DEBUG -- Tread cleaner is alive ...  -- "% (strftime('%c',localtime())) )

            #Prochain check dans x minutes
            sleep(conf.getint("GLOBAL","CHECK_THREAD_TIMER"))


        

