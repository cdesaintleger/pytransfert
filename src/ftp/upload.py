# -*- coding: utf8 -*-

#multitâche
from threading import Thread

#ftp
#from ftplib import FTP, error_perm, all_errors
from libftputil import ftputil


#logging
from time import strftime, gmtime

#ftp
from bdd import acces_bd


#mails
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MyFtp(Thread):

    def __init__(self,sem,file,logger,conf):

        #initialisation du thread
        Thread.__init__(self)

        #lecture du fichier de config
        self.conf    =   conf

        #recupération de jetons ( semaphore )
        self.sem = sem

        #recupre le fichier à uploader
        self.file   =   file

        #Connexion SQL pour la changement des etats

        #instanciation à la base
        self.sql  =   acces_bd.Sql()

        #Paramétres de connection
        self.sql.set_db(conf.get("DDB", "DATABASE"))
        self.sql.set_host(conf.get("DDB", "HOST"))
        self.sql.set_user(conf.get("DDB", "USER"))
        self.sql.set_password(conf.get("DDB", "PASSWORD"))
        #connection effective
        self.sql.conn()
        

        #mise en place du logger
        self.logger=logger

    #action du thread ( start )
    def run(self):

        #Signale que l'on met en file le fichier
        self.logger.info("%s -- INFO -- Attente du thread -- %s"% (strftime('%c',gmtime()), self.file[1]) )
        
        #aquisition d'un jeton ( semaphore ) ou attente d'une libération
        self.sem.acquire()

        #Changement d'état en base => 2 upload en cours
        self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 2 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

        try:
            #jeton acquis , signalement du lancement de l'upload du fichier
            self.logger.info("%s -- INFO -- Execution du thread -- %s"% (strftime('%c',gmtime()), self.file[1]) )

            #envoie du fichier au module FTP
            cret = self._send_file()
            
            #test du code retour 0 = OK
            if(cret != 1):
                try:
                    #Changement d'état en base => 3 upload terminé si tout est ok
                    self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 3 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

                except MySQLdb.Error, e: 
                    self.logger.info( "%s -- ERR -- Error %d: %s" % (strftime('%c',gmtime()), e.args[0], e.args[1]) )

                #notification 
                self.notify_by_mail('data_newfilenotify')
                self.logger.info("%s -- INFO -- Notify new file -- %s"% (strftime('%c',gmtime()), self.file[1]) )

            else:
                #remet l'etat du fichier à 0 pour reesayer
                self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 0 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

                #notification 
                self.notify_by_mail('data_retryfilenotify')
                self.logger.info("%s -- ERR -- Notify retry file -- %s"% (strftime('%c',gmtime()), self.file[1]) )

        finally:
            #signalement de la fin de l'upload donc du thread
            self.logger.info("%s -- INFO -- Fin du thread -- %s"% (strftime('%c',gmtime()), self.file[1]) )

            #libération du jeton pour laisser la place à un autre
            self.sem.release()


    def keepalive(self,ftp):
        ftp.keep_alive()

    #Méthode _send_file gére les transactions avec le serveur FTP 
    def _send_file(self):


        try:

            self.logger.info("%s -- INFO -- Connexion -- %s"% (strftime('%c',gmtime()), self.file[4]) )
            ftp =   ftputil.FTPHost( self.conf.get("FTP", "HOST"), self.conf.get("FTP", "USER"), self.conf.get("FTP", "PASSWORD"))
            
            try:
                #creation du repertoire destination
                self.logger.info("%s -- INFO -- Creation repertoire -- %s"% (strftime('%c',gmtime()), self.file[4]) )
                ftp.mkdir(str(self.file[4]).strip('/'))

            except OSError, resp:

                #si le repertoire existe déjà .. on signale et on passe
                self.logger.info("%s -- WARN -- Repertoire deja existant -- %s"% (strftime('%c',gmtime()), self.file[1]) )

            finally:

                #on se déplace dans le repertoire finale
                ftp.chdir(self.file[4])

                #info du lancement d'upload du fichier
                self.logger.info("%s -- INFO -- Depot du fichier -- %s"% (strftime('%c',gmtime()), self.file[1]) )
                
                try:

                    #Lancement de l'upload proprement dit#
                    ftp.upload(self.file[3]+self.file[1], self.file[1], 'b', self.keepalive(ftp))

                    #info du lancement d'upload du fichier
                    self.logger.info("%s -- INFO -- Depot terminé -- %s"% (strftime('%c',gmtime()), self.file[1]) )

                except FTPIOError, resp:

                    #remet l'etat du fichier à 0 pour reesayer
                    self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 0 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

                    self.logger.info("%s -- ERR -- %s etat devient 0 Erreur transfert du fichier -- %s"% (strftime('%c',gmtime()), resp, self.file[1]) )
                    #Retour erreur
                    return 1

            #code retour
            return 0
        
        except:

            #Changement d'état en base => 500 Probleme de connection ou d'ecriture
            self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 500 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

            self.logger.info("%s -- ERR -- etat devient 500 -- %s"% (strftime('%c',gmtime()), self.file[1]) )

            #notification erreur
            self.logger.info("%s -- INFO -- Notify error -- %s"% (strftime('%c',gmtime()), self.file[1]) )
            self.notify_by_mail('data_emergencynotify')

            return 1
        
        finally:

            self.logger.info("%s -- INFO -- Deconnection du FTP -- %s"% (strftime('%c',gmtime()), self.file[1]) )
            ftp.close()


    #Notification par mail de l'arrivé des fichiers ou d'un probléme quelconque
    def notify_by_mail(self,mail_type):

        maildata    =   self._dispatch(mail_type)
        
        # me == my email address
        # you == recipient's email address
        me = maildata.get("from","pytransfert@rapid-flyer.com")

        #recupére les adresses selon le type de mail à envoyer ( data_newfilenotify , data_emergencynotify )
        you =   maildata.get("destinataires","cdesaintleger@creavi.fr")


        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = maildata.get("sujet","notification pytransfert")
        msg['From'] = me
        msg['To'] = you

        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(maildata.get("text","notification pytransfert"), 'plain')
        part2 = MIMEText(maildata.get("html","notification pytransfert"), 'html')

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)

        # Send the message via local SMTP server.
        s = smtplib.SMTP('localhost')
        # sendmail function takes 3 arguments: sender's address, recipient's address
        # and message to send - here it is sent as one string.
        s.sendmail(me, you, msg.as_string())
        s.quit()




    #Recupére les infos pour l'expédition de mail à l'arrive d'un nouveau fichier
    def _data_newfilenotify(self):

        #definition du dictionaire
        data    =   {}

        data['from']    =   str(self.conf.get("NOTIFY","NEWFILEFROM"))
        data['destinataires']    =   str(self.conf.get("NOTIFY","NEWFILEDEST"))
        data['sujet']           =   "CMD "+str(self.file[4])+" : " + str(self.conf.get("NOTIFY","NEWFILESUBJECT"))

        # Create the body of the message (a plain-text and an HTML version).
        data['text'] = "Hi!\nHow are you?\nCommande N° "+str(self.file[4])+" Un nouveau fichier client est arrivé : " +str(self.file[1])+ "\nIl se trouve dans le répertoire réseau pytransfert"+str(self.file[4])

        data['html'] = """\
        <html>
          <head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
          </head>
          <body>
            <p>Hi!<br>
               How are you?<br>
               <h3>Commande N° """+str(self.file[4])+"""</h3>
               Un nouveau fichier client est arrivé : <b>""" +str(self.file[1])+ """</b><br>
               Il se trouve dans le répertoire réseau <b>pytransfert"""+str(self.file[4])+"""</b>
            </p>
          </body>
        </html>
        """

        return data

    #Recupére les infos pour l'expédition de mail à l'arrive d'un probléme
    def _data_emergencynotify(self):

        #definition du dictionaire
        data    =   {}

        data['from']    =   str(self.conf.get("NOTIFY","EMERGENCYFROM"))
        data['destinataires']    =   str(self.conf.get("NOTIFY","EMERGENCYDEST"))
        data['sujet']           =   str(self.conf.get("NOTIFY","EMERGENCYSUBJECT"))

        # Create the body of the message (a plain-text and an HTML version).
        data['text'] = "Erreur de transfert Pytransfert\nMerci de vous rapprocher de christophe cdesaintleger@creavi.fr pour vérifier les connexions "
        data['html'] = """\
        <html>
          <head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
          </head>
          <body>
            <p>Hi!<br>
               Un probléme vient d'être signalé sur le transfert d'un fichier .<br>
               Merci de vérifier la connexion au serveur FTP<br>
               <b>83.206.237.107</b><br/><br/>
               <h3>Commande N° """+str(self.file[4])+"""</h3>
               Fichier concerné : <b>""" +str(self.file[1])+ """</b><br>
            </p>
          </body>
        </html>
        """

        return data

    #Recupére les infos pour l'expédition de mail à l'arrive d'un probléme pendant le transfert
    def _data_retryfilenotify(self):

        #definition du dictionaire
        data    =   {}

        data['from']    =   str(self.conf.get("NOTIFY","EMERGENCYFROM"))
        data['destinataires']    =   str(self.conf.get("NOTIFY","EMERGENCYDEST"))
        data['sujet']           =   str(self.conf.get("NOTIFY","EMERGENCYSUBJECT"))

        # Create the body of the message (a plain-text and an HTML version).
        data['text'] = "Erreur pedant transfert Pytransfert\nMerci de vous rapprocher de christophe cdesaintleger@creavi.fr pour vérifier les connexions "
        data['html'] = """\
        <html>
          <head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
          </head>
          <body>
            <p>Hi!<br>
               Un probléme vient d'être signalé sur le transfert d'un fichier .<br>
               Merci de vérifier la connexion au serveur FTP<br>
               <b>83.206.237.107</b><br/><br/>
               <h3>Commande N° """+str(self.file[4])+"""</h3>
               Fichier concerné : <b>""" +str(self.file[1])+ """</b><br>
            </p>
          </body>
        </html>
        """

        return data



    #######################################
    #
    #           DISPATCH
    #
    #######################################



    #emule le switch case .. qui n'hexiste pas en python
    def _dispatch (self, value):

        method_name = '_' + str(value)
        method = getattr(self, method_name)
        return method()




    


