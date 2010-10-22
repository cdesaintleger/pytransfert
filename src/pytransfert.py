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


#############################################
##                                         ##
##                  MAIN                   ##
##                                         ##
#############################################

def maintimer(tempo):

    #Timer par defaut */5 minutes
    threading.Timer(tempo, maintimer, [tempo]).start()

    #lecture du fichier de config
    conf    =   ConfigParser.ConfigParser()
    conf.read("params.ini")
    
    #instanciation à la base
    sql  =   acces_bd.Sql()

    #Paramétres de connection
    sql.set_db(conf.get("DDB", "DATABASE"))
    sql.set_host(conf.get("DDB", "HOST"))
    sql.set_user(conf.get("DDB", "USER"))
    sql.set_password(conf.get("DDB", "PASSWORD"))
    #connection effective
    sql.conn()

    #Recupére les images à transferer
    res =   sql.execute("SELECT * FROM ps_bec_liens_crea_cmd WHERE transfert = 0", "select")
    
    #On marque tout ces fichiers comme "En file"
    #parcour des fichiers
    for file in res:
        print file
        #ici requete à la ddb pour l'update 


    #
    #   Pour les test je vais inserer
    #   Manuellement les images à uploader dans le tuple
    #
    res = res + ('a.jpg','b.jpg','c.jpg')

    #compte le nombre de resultats trouvés
    nbFiles =   len(res)
    
    #lancement uniquement sil y a des fichiers à uploader
    if( nbFiles > 0 ):

        #lancement des transfert par thread
        trans   =   launch.Transfert()
        #Envoie la file à gerer
        trans.upload_ftp(res)




#############################################
##                                         ##
##      Lancement De la boucle infinie     ##
##                                         ##
#############################################
if __name__ == "__main__":

    #lecture du fichier de config
    conf    =   ConfigParser.ConfigParser()
    conf.read("params.ini")

    #Lancement main go go go 
    maintimer(conf.getint("GLOBAL", "TIMER"))
    


