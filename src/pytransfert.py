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


#############################################
##                                         ##
##                  MAIN                   ##
##                                         ##
#############################################

def maintimer(tempo = 350):

    #Timer par defaut */5 minutes
    threading.Timer(tempo, maintimer, [tempo]).start()

    #connection à la base
    sql  =   acces_bd.Sql()

    #Paramétres de connection
    sql.set_db("")
    sql.set_host("localhost")
    sql.set_user("")
    sql.set_password("")
    #connection effective
    sql.conn()

    #Recupére les images à transferer
    res =   sql.execute("SELECT * FROM ps_bec_liens_crea_cmd WHERE transfert = 0", "select")
    #compte le nombre de resultats trouvés
    nbFiles =   len(res)


    #
    #   Pour les test je vais inserer
    #   Manuellement les images à uploader dans le tuple
    #
    res = res + ('a.jpeg','b.jpag','c.jpeg')

    #lancement uniquement sil y a des fichiers à uploader
    if( nbFiles > 0 ):
        #lancement des transfert par thread
        trans   =   launch.Transfert()
        #Envoie la file à gerer
        trans.uploadFTP(res)




#############################################
##                                         ##
##      Lancement De la boucle infinie     ##
##                                         ##
#############################################
if __name__ == "__main__":

    #Lancement main go go go 
    maintimer(3)

    


