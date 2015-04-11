# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 15:01:17 2015

@author: napo
"""
from __future__ import unicode_literals

import requests
import sqlite3
from io import BytesIO
from lxml import etree

def createtablelotti():
    create = ""
    create = """
        CREATE TABLE IF NOT EXISTS lotti (id INTEGER, cig TEXT, cf_proponente TEXT, proponente TEXT,
        oggetto TEXT, cf_aggiudicatario TEXT, aggiudicatario TEXT, tipo_contratto INTEGER, importo REAL,datainizio TIMESTAMP,
        datafine TIMESTAMP,sommaliquidata REAL);
    """
    return str(create)

def createtablepartecipanti():
    create = ""
    create = """
        CREATE TABLE IF NOT EXISTS partecipanti 
        (idlotto INTEGER, cig TEXT, codicefiscale TEXT, ragionesociale TEXT);
    """
    return str(create)

def createtabletipocontratti():
    create = ""
    create = """
        CREATE TABLE IF NOT EXISTS tipo_contratti 
        (idtipo INTEGER, contratto TEXT);
    """   
    return str(create)
    
def idtipocontratto(s):
    v = int(s.split("-")[0])
    return v

def tipocontratto(s):
    sr = s.split("-")
    v = ""
    for i in range(1,len(sr)):
        v = v + " " + sr[i]
    return v
    
xml = requests.get('http://www.agid.gov.it/sites/default/files/documentazione/agiddataset2014.xml')
#tree = etree.parse(xml.content)
tree = etree.parse(BytesIO(xml.content))
titolo = tree.findall('metadata/titolo')[0].text
abstract = tree.findall('metadata/abstract')[0].text
datapubbicazione = tree.findall('metadata/dataPubbicazioneDataset')[0].text
entepubblicatore = tree.findall('metadata/entePubblicatore')[0].text
ultimoaggiornamento = tree.findall('metadata/dataUltimoAggiornamentoDataset')[0].text
annoriferimento = tree.findall('metadata/annoRiferimento')[0].text
source = tree.findall('metadata/urlFile')[0].text
licenza = tree.findall('metadata/licenza')[0].text
lotti = tree.findall('data/lotto')
records = []
contratti = []
for lotto in lotti:
    data = {}
    data['cig']=lotto.find('cig').text
    proponente = lotto.find('strutturaProponente')
    data['cf_proponente'] = proponente[0].text
    denominazione = ''
    if proponente[1] is not None:
        denominazione = proponente[1].text
    data['proponente'] = denominazione
    data['oggetto'] = lotto.find('oggetto').text
    contratto = lotto.find('sceltaContraente').text
    data['tipo_contratto']=idtipocontratto(contratto)
    contratti.append(contratto)
    pp = lotto.find('partecipanti')
    partecipanti = []
    for p in pp:
        partecipante = {}
        if p is not None:
            codicefiscale = p.find('codiceFiscale')
            ragionesociale = p.find('ragioneSociale')
            partecipante['codicefiscale'] = ''
            partecipante['ragionesociale'] = ''
            if codicefiscale is not None:
                partecipante['codicefiscale'] = codicefiscale.text
            if ragionesociale is not None:
                partecipante['ragionesociale'] = ragionesociale.text
        partecipanti.append(partecipante)
    data['partecipanti'] = partecipanti
    agg = lotto.find('aggiudicatari')
    aggiudicatari = []
    for ag in agg:
        aggiudicatario = {}
        aggiudicatario['codicefiscale'] = '' 
        cf = ag.find('codiceFiscale')
        if cf is not None:
            aggiudicatario['codicefiscale'] = cf.text
        aggiudicatario['ragionesociale'] = ''
        ragionesociale = ag.find('ragioneSociale')
        if ragionesociale is not None:
            aggiudicatario['ragionesociale'] = ragionesociale.text
        aggiudicatari.append(aggiudicatario)
    data['aggiudicatari'] = aggiudicatari
    if len(aggiudicatari) > 0:
        data['aggiudicatario'] = aggiudicatari[0]['ragionesociale']
        data['cf_aggiudicatario'] = aggiudicatari[0]['codicefiscale']
    else:
        data['aggiudicatario'] = ""
        data['cf_aggiudicatario'] = ""      
    data['importo'] = lotto.find('importoAggiudicazione').text
    tempi = lotto.find('tempiCompletamento')
    data['datainizio']= ''
    data['dataultimazione'] = ''
    datainizio = tempi.find('dataInizio')
    dataultimazione = tempi.find('dataUltimazione')
    if datainizio is not None:
        data['datainizio'] = datainizio.text
    if dataultimazione is not None:
        data['dataultimazione'] = dataultimazione.text
    data['sommeliquidata'] = lotto.find('importoSommeLiquidate').text
    records.append(data)


dbout = 'legge190.sqlite'
k=0
con=sqlite3.connect(dbout)        
con.enable_load_extension(True)
cur = con.cursor()
cur.execute(createtablelotti())
cur.execute(createtablepartecipanti())
cur.execute(createtabletipocontratti())

for record in records:
    cig = record['cig']
    cur.execute('''INSERT INTO lotti (id, cig, cf_proponente, proponente, oggetto, cf_aggiudicatario, aggiudicatario, tipo_contratto,importo, datainizio, datafine, sommaliquidata)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (k,cig,record['cf_proponente'], record['proponente'], record['oggetto'], record['cf_aggiudicatario'], record['aggiudicatario'],record['tipo_contratto'],record['importo'], record['datainizio'], record['dataultimazione'], record['sommeliquidata']))
    con.commit()
    for r in record['partecipanti']:
            if r['ragionesociale'] != '':
                cur.execute('''INSERT INTO partecipanti (idlotto, cig, codicefiscale, ragionesociale) VALUES(?,?,?,?)''', (k,cig, r['codicefiscale'], r['ragionesociale'])) 
                con.commit()
    k = k+1
for u in tuple(set(contratti)):
    idt = idtipocontratto(u)
    tipo = tipocontratto(u)
    cur.execute('''INSERT INTO tipo_contratti (idtipo, contratto) VALUES (?,?)''', (idt,tipo))
con.commit()
con.close()
