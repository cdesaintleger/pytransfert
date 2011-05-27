#!/usr/bin/python
# -*- coding: utf8 -*-

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="christophe de saint leger"
__date__ ="$29 mars 2011 10:46:48$"

#importation des lib necessaires
from bdd import acces_bd
from transfert import launch
import threading
import ConfigParser
import os,sys
import warnings

#logging
from time import strftime, gmtime

#logging
import logging
import logging.handlers

  
#############################################
##                                         ##
##                  MAIN                   ##
##                                         ##
#############################################

def maintimer(tempo, trans, conf, logger):

    global gl_rotation_ftp
    gl_rotation_ftp =   gl_rotation_ftp+1

    if ((gl_rotation_ftp)%conf.getint("GLOBAL","NB_CYCLE_ROTATION_FTP") == 0) & (conf.getint("GLOBAL","ENABLE_ROTATION") == 1):

        #Fin du fils on le fork
        pid = os.fork()
        if pid:
            #Log de la rotation
            logger.info("%s -- INFO -- Rotation du process Ftp -- "% (strftime('%c',gmtime())) )
            sys.exit(os.EX_OK)

        else:
            logger.info("%s -- INFO -- Je suis le nouveau process Ftp -- "% (strftime('%c',gmtime())) )
            gl_rotation_ftp = 0
            maintimer(tempo, trans, conf, logger)

    else:
        
        #instanciation à la base
        sql  =   acces_bd.Sql()

        #Paramétres de connection
        sql.set_db(conf.get("DDB", "DATABASE"))
        sql.set_host(conf.get("DDB", "HOST"))
        sql.set_user(conf.get("DDB", "USER"))
        sql.set_password(conf.get("DDB", "PASSWORD"))
        #connection effective
        sql.conn()



        #Timer par defaut */5 minutes
        threading.Timer(tempo, maintimer, [tempo,trans,conf,logger]).start()

        #Recupére les images à transferer ( nouvelles + écouées )
        res =   sql.execute("\
            SELECT "+str(conf.get("DDB","CHAMP_ID"))+",\
            "+str(conf.get("DDB","CHAMP_IMG"))+",\
            "+str(conf.get("DDB","CHAMP_CMD"))+",\
            "+str(conf.get("DDB","CHAMP_SOURCE"))+",\
            "+str(conf.get("DDB","CHAMP_DEST"))+"\
            FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
            WHERE "+str(conf.get("DDB","CHAMP_ETAT"))+" in (0,500)")


        #parcour des fichiers
        listeid =  list()

        for file in res:
            listeid.append(str(file[0]))

        #compte le nombre de resultats trouvés
        nbFiles =   len(res)

        #lancement uniquement sil y a des fichiers à uploader
        if( nbFiles > 0 ):

            #On marque tout ces fichiers comme "En file"
            sql.execute("UPDATE "+str(conf.get("DDB","TBL_ETAT"))+" SET "+str(conf.get("DDB","CHAMP_ETAT"))+" = 1 WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+','.join(listeid)+")")


            #Envoie la file à gerer
            trans.upload_ftp(res,logger,conf)



#Méthode de nettoyage des fichiers uploadés
def cleaner_timer(tempo,conf):

    global gl_rotation_clean
    gl_rotation_clean   =   gl_rotation_clean+1

    if ((gl_rotation_clean)%conf.getint("GLOBAL","NB_CYCLE_ROTATION_CLEANER") == 0) & (conf.getint("GLOBAL","ENABLE_ROTATION") == 1):

        #Fin du fils on le fork
        pid = os.fork()
        if pid:
            logger.info("%s -- INFO -- Rotation du process Cleaner -- "% (strftime('%c',gmtime())) )
            sys.exit(os.EX_OK)

        else:
            logger.info("%s -- INFO -- je suis le nouveau process Cleaner -- "% (strftime('%c',gmtime())) )
            gl_rotation_clean = 0
            cleaner_timer(tempo,conf)

    else:
        #instanciation à la base
        sql  =   acces_bd.Sql()

        #Paramétres de connection
        sql.set_db(conf.get("DDB", "DATABASE"))
        sql.set_host(conf.get("DDB", "HOST"))
        sql.set_user(conf.get("DDB", "USER"))
        sql.set_password(conf.get("DDB", "PASSWORD"))
        #connection effective
        sql.conn()

        #Timer par defaut */5 minutes
        threading.Timer(tempo, cleaner_timer, [tempo,conf]).start()

        #Recupére les images à transferer ( nouvelles + écouées )
        res =   sql.execute("\
            SELECT \
            "+str(conf.get("DDB","CHAMP_ID"))+",\
            "+str(conf.get("DDB","CHAMP_IMG"))+",\
            "+str(conf.get("DDB","CHAMP_SOURCE"))+"\
            FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
            WHERE "+str(conf.get("DDB","CHAMP_ETAT"))+" in (3)\
            AND TO_DAYS( NOW() ) - TO_DAYS("+str(conf.get("DDB","CHAMP_DATE"))+") > "+str(conf.get("GLOBAL","JOURS_RETENTION")) )

        if( len(res) > 0 ):
            for file in res:

                print ("Nettoyage de : "+str(file[2])+"/"+str(file[1])+"\n")

                try:

                    os.remove(str(file[2])+"/"+str(file[1]))
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
        print ">>> Le Pére: Fils ou es tu ? ... je te quitte nous nous retrouverons au prochain reboot ..."
        sys.exit(os.EX_OK)

    else:
        print ">>> Le Fils ( pid: "+str(pid)+" ): Pére je suis là ... je vais méner la mission à bien ne t'en fait pas ... mon créaeur est un génie .. ' oulà .. les chevilles :p  '"

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



        #Instanciation du transfert par thread
        trans   =   launch.Transfert()

        #Lancement main go go go
        maintimer(conf.getint("GLOBAL", "TIMER"), trans, conf, logger)

        #Gestion du nettoyae automatique
        cleaner_timer(conf.getint("GLOBAL","CLEANER_TIMER"),conf)

        

