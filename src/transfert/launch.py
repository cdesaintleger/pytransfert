# -*- coding: utf8 -*-
from threading import Thread, BoundedSemaphore
from time import sleep

#tableau des threads
ThUp    =   {}

#nombre de process en paralelle
__MaxSem__ = 10

#définition du sémaphore
sem =   BoundedSemaphore(__MaxSem__)


class Transfert(Thread):



    def uploadFTP(self,list):

        
        #parcour des fichiers
        for file in list:

            #upload du fichier par un thread
            ThUp[file]  =   Thread(name=file,target=ftp.send,args=(file,))

            #prend un jeton
            sem.acquire()
            # attente d'un  sem.release()

            #lancement de l'upload
            ThUp[file].start()


        #tant qu'il y a des threads actifs
        while len(ThUp) > 0:

            #liste des thread à purger
            Thfinished  =   list()
            nbrestant   =   len(ThUp)

            #Liste les threads restants
            for th in ThUp:

                #Test si le thread X est encore en action
                if ThUp[th].isAlive() == 0:
                    #Il est terminé , on decomte le nombre de thread restant
                    nbrestant   =   nbrestant-1
                    #on l'inscrit pour une suppression du tableau des thread actifs
                    Thfinished.append(th)

            #purge des thread terminés
            for adl in Thfinished:
                #retrait de la liste
                del(ThUp[adl])

            #pause avant prochaine itération
            sleep(1)
