#!/usr/bin/python
# -*- coding: utf8 -*-

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="christophe de saint leger"
__date__ ="$21 oct. 2010 13:34:48$"

#importation des lib necessaires
from bdd import acces_bd
from transfert import launch
import threading
import ConfigParser
import os
import warnings

#############################################
##                                         ##
##                  MAIN                   ##
##                                         ##
#############################################

def maintimer(tempo, trans, conf):

    #Timer par defaut */5 minutes
    threading.Timer(tempo, maintimer, [tempo,trans,conf]).start()
    
    #instanciation à la base
    sql  =   acces_bd.Sql()

    #Paramétres de connection
    sql.set_db(conf.get("DDB", "DATABASE"))
    sql.set_host(conf.get("DDB", "HOST"))
    sql.set_user(conf.get("DDB", "USER"))
    sql.set_password(conf.get("DDB", "PASSWORD"))
    #connection effective
    sql.conn()

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
        trans.upload_ftp(res)



#Méthode de nettoyage des fichiers uploadés
def cleaner_timer(tempo,conf):

    #Timer par defaut */5 minutes
    threading.Timer(tempo, cleaner_timer, [tempo,conf]).start()

    #instanciation à la base
    sql  =   acces_bd.Sql()

    #Paramétres de connection
    sql.set_db(conf.get("DDB", "DATABASE"))
    sql.set_host(conf.get("DDB", "HOST"))
    sql.set_user(conf.get("DDB", "USER"))
    sql.set_password(conf.get("DDB", "PASSWORD"))
    #connection effective
    sql.conn()

    #Recupére les images à transferer ( nouvelles + écouées )
    res =   sql.execute("\
        SELECT "+str(conf.get("DDB","CHAMP_ID"))+"\
        "+str(conf.get("DDB","CHAMP_IMG"))+",\
        "+str(conf.get("DDB","CHAMP_SOURCE"))+"\
        FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
        WHERE "+str(conf.get("DDB","CHAMP_ETAT"))+" in (3)\
        AND TO_DAYS( NOW() ) - TO_DAYS("+str(conf.get("DDB","CHAMP_DATE"))+") > "+str(conf.get("GLOBAL","JOURS_RETENTION")) )

    if( len(res) > 0 ):
        for file in res:
            try:
                os.remove(file[2]+file[1])
                #On marque tout ces fichiers comme "nettoyé"
                sql.execute("UPDATE "+str(conf.get("DDB","TBL_ETAT"))+"\
                SET "+str(conf.get("DDB","CHAMP_ETAT"))+" = 33 \
                WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+file[0]+")")
            except:
                #marque le fichier comme impossible à nettoyer
                sql.execute("UPDATE "+str(conf.get("DDB","TBL_ETAT"))+"\
                SET "+str(conf.get("DDB","CHAMP_ETAT"))+" = 304 \
                WHERE "+str(conf.get("DDB","CHAMP_ID"))+" in ("+file[0]+")")
                warnings.warn("Impossible de supprimer un fichier !")








#############################################
##                                         ##
##      Lancement De la boucle infinie     ##
##                                         ##
#############################################
if __name__ == "__main__":

    #lecture du fichier de config
    conf    =   ConfigParser.ConfigParser()
    conf.read("params.ini")

    #Instanciation du transfert par thread
    trans   =   launch.Transfert()

    #Lancement main go go go 
    maintimer(conf.getint("GLOBAL", "TIMER"), trans, conf)
    
    #Gestion du nettoyae automatique
    cleaner_timer( conf.getint("GLOBAL","CLEANER_TIMER"),conf )


