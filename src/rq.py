# -*- coding: utf8 -*-

def get_new_files(sql,conf):
    """
    Récupére les nouveaux fichiers à traiter
    """
    #Recupére les images à transferer ( nouvelles + écouées )
    res =   sql.execute("\
        SELECT "+str(conf.get("DDB","CHAMP_ID"))+",\
        "+str(conf.get("DDB","CHAMP_IMG"))+",\
        "+str(conf.get("DDB","CHAMP_CMD"))+",\
        "+str(conf.get("DDB","CHAMP_SOURCE"))+",\
        "+str(conf.get("DDB","CHAMP_DEST"))+"\
        FROM "+str(conf.get("DDB","TBL_ETAT"))+"\
        WHERE "+str(conf.get("DDB","CHAMP_ETAT"))+" in (0,500)")
    
    return res




def set_state(sql,conf,ids,state):
    """
    Change les etats des enregistrements dont les ids sont = ids
    """
    sql.execute("UPDATE \
                "+str(conf.get("DDB","TBL_ETAT"))+"\
                SET \
                "+str(conf.get("DDB","CHAMP_ETAT"))+" = 1 \
                WHERE \
                "+str(conf.get("DDB","CHAMP_ID"))+" in ("+','.join(ids)+")")