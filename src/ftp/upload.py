# -*- coding: utf8 -*-

#multitâche
from threading import Thread

#ftp
from ftplib import FTP, error_perm, all_errors

#log
from Debug import Debug

#Fichier de config
import ConfigParser

#ddb
from bdd import acces_bd

#mails
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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
            cret = self._send_file()
            
            #test du code retour 0 = OK
            if(cret == 0):
                #Changement d'état en base => 3 upload terminé si tout est ok
                self.sql.execute("UPDATE "+str(self.conf.get("DDB","TBL_ETAT"))+" SET "+str(self.conf.get("DDB","CHAMP_ETAT"))+" = 3 WHERE "+str(self.conf.get("DDB","CHAMP_ID"))+" = "+str(self.file[0]))

                #notification 
                self.notify_by_mail('data_newfilenotify')

        finally:
            #signalement de la fin de l'upload donc du thread
            print "Fin du thread => ", self.file[1]

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

            #code retour
            return 0
        
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
        
        finally:

            #fermeture du fichier
            f.close()
            #cloture de la connection FTP
            ftp.quit()


    #Notification par mail de l'arrivé des fichiers ou d'un probléme quelconque
    def notify_by_mail(self,mail_type):


        maildata    =   self._dispatch(mail_type)

        # me == my email address
        # you == recipient's email address
        me = maildata.get("from","pytransfert@rapid-flyer.com")

        #recupére les adresses selon le type de mail à envoyer ( data_newfilenotify , data_emergencynotify )
        you =   maildata.get("destinataire","pytransfert@rapid-flyer.com")


        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = maildata.get("sujet","notification pytransfert")
        msg['From'] = me
        msg['To'] = you

        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')

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

            data['from']    =   str(self.conf.get("NOTIFY","NEWFILEFROM"))
            data['destinataires']    =   str(self.conf.get("NOTIFY","NEWFILEDEST"))
            data['sujet']           =   str(self.conf.get("NOTIFY","NEWFILESUBJECT"))

            # Create the body of the message (a plain-text and an HTML version).
            data['text'] = "Hi!\nHow are you?\nHere is the link you wanted:\nhttp://www.python.org"
            data['html'] = """\
            <html>
              <head></head>
              <body>
                <p>Hi!<br>
                   How are you?<br>
                   Here is the <a href="http://www.python.org">link</a> you wanted.
                </p>
              </body>
            </html>
            """

            return data

        #Recupére les infos pour l'expédition de mail à l'arrive d'un probléme 
        def _data_emergencynotify(self):

            data['from']    =   str(self.conf.get("NOTIFY","EMERGENCYFROM"))
            data['destinataires']    =   str(self.conf.get("NOTIFY","EMERGENCYDEST"))
            data['sujet']           =   str(self.conf.get("NOTIFY","EMERGENCYSUBJECT"))

            # Create the body of the message (a plain-text and an HTML version).
            data['text'] = "Hi!\nHow are you?\nHere is the link you wanted:\nhttp://www.python.org"
            data['html'] = """\
            <html>
              <head></head>
              <body>
                <p>Hi!<br>
                   How are you?<br>
                   Here is the <a href="http://www.python.org">link</a> you wanted.
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




    


