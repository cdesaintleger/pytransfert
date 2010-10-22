# -*- coding: utf8 -*-
from threading import Thread, BoundedSemaphore
from time import sleep
import ConfigParser
from ftp import upload


class Transfert(Thread):
    
        
    


    def __init__(self):

        #lecture du fichier de config
        self.conf    =   ConfigParser.ConfigParser()
        self.conf.read("params.ini")

        #définition du sémaphore
        self.sem =   BoundedSemaphore(self.conf.getint("GLOBAL","NBTHREAD"))

        #tableau des threads
        self.ThUp    =   {}

        
    def upload_ftp(self,list):

        
        #parcour des fichiers
        for file in list:
            
            #upload du fichier par un thread
            self.ThUp[file]  =   Thread(name=file,target=upload.send_file,args=(self.sem,file,"/indamix",))

            #prend un jeton
            self.sem.acquire()
            # attente d'un  sem.release()

            #lancement de l'upload
            self.ThUp[file].start()

            #Juste pour les logs
            print "Upload lancé pour ==> ", file

        #tant qu'il y a des threads actifs
        while len(self.ThUp) > 0:

            #liste des thread à purger
            Thfinished  =   []
            nbrestant   =   len(self.ThUp)

            print "Threads en action ... ", nbrestant

            #Liste les threads restants
            for th in self.ThUp:

                #Test si le thread X est encore en action
                if self.ThUp[th].isAlive() == 0:
                    #Il est terminé , on decomte le nombre de thread restant
                    nbrestant   =   nbrestant-1
                    #on l'inscrit pour une suppression du tableau des thread actifs
                    Thfinished.append(th)

            #purge des thread terminés
            for adl in Thfinished:
                #retrait de la liste
                del(self.ThUp[adl])

            #pause avant prochaine itération
            sleep(1)
