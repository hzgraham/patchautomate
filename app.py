import os
import psycopg2
import time
import xmlrpc.client, xmlrpc.server
import logging
import sys

def config():
    service_name = os.getenv('DATABASE_SERVICE_NAME', '').upper()
    return {
        'DATABASE_NAME': os.getenv('DATABASE_NAME'),
        'DATABASE_USER': os.getenv('DATABASE_USER'),
        'DATABASE_PASSWORD': os.getenv('DATABASE_PASSWORD'),
        'DATABASE_HOST': os.getenv('{}_SERVICE_HOST'.format(service_name)),
        'DATABASE_PORT': os.getenv('{}_SERVICE_PORT'.format(service_name)),
        'SATELLITE_USER': os.getenv('SATELLITE_USER'),
        'SATELLITE_PASSWORD': os.getenv('SATELLITE_PASSWORD'),
        'SATELLITE_HOST': os.getenv('SATELLITE_HOST')
    }

def getSatelliteId(hostname):
    cfg = config()
    URL = "https://" + cfg['SATELLITE_HOST']  + "/rpc/api"
    client = xmlrpc.client.Server(URL, verbose=0)
    session = client.auth.login(cfg['SATELLITE_USER'], cfg['SATELLITE_PASSWORD'])

    client = xmlrpc.client.Server(URL, verbose=0)
    data = client.system.getId(session, hostname)
    if data:
        return data[0].get('id')
    else:
        return None

def desiredErrata(updates, RHEA, RHSA, RHBA):
    advisories = ['RHEA','RHSA','RHBA']
    needed_updates = []
    errata_levels = {'rhea': RHEA,
                     'rhsa': RHSA,
                     'rhba': RHBA }

    if errata_levels:
        if errata_levels['rhea']:
            rhea_date = errata_levels['rhea'].split('-')[1].split(':')[0]
            rhea_id = errata_levels['rhea'].split('-')[1].split(':')[1]
        else:
            rhea_date = 0
            rhea_id = 0

        if errata_levels['rhsa']:
            rhsa_date = errata_levels['rhsa'].split('-')[1].split(':')[0]
            rhsa_id = errata_levels['rhsa'].split('-')[1].split(':')[1]
        else:
            rhsa_date = 0
            rhsa_id = 0

        if errata_levels['rhba']:
            rhba_date = errata_levels['rhba'].split('-')[1].split(':')[0]
            rhba_id = errata_levels['rhba'].split('-')[1].split(':')[1]
        else:
            rhba_date = 0
            rhba_id = 0

        #Iteritively checks each available errata with the errata level
        for each in updates:
            if any(x in each for x in advisories):
                adv_type = each.split('-')[0]
                date = each.split('-')[1].split(':')[0]
                errata_id = each.split('-')[1].split(':')[1]
                #If the available errata is equal to or older than the level
                #it is added to the needed_updates list
                #and it be saved as Server.plerrata
                if adv_type == 'RHEA':
                    if date < rhea_date:
                        needed_updates.append(each)
                    elif date <= rhea_date and errata_id <= rhea_id:
                        needed_updates.append(each)
                if adv_type == 'RHSA':
                    if date < rhsa_date:
                        needed_updates.append(each)
                    elif date <= rhsa_date and errata_id <= rhsa_id:
                        needed_updates.append(each)
                if adv_type == 'RHBA':
                    if date < rhba_date:
                        needed_updates.append(each)
                    elif date <= rhba_date and errata_id <= rhba_id:
                        needed_updates.append(each)
        return needed_updates

def getErrataForSatId(satid):
    cfg = config()
    URL = "https://" + cfg['SATELLITE_HOST']  + "/rpc/api"
    client = xmlrpc.client.Server(URL, verbose=0)
    session = client.auth.login(cfg['SATELLITE_USER'], cfg['SATELLITE_PASSWORD'])

    client = xmlrpc.client.Server(URL, verbose=0)
    errata = client.system.getRelevantErrata(session, satid)
    updates = []
    if errata:
        for erratum in errata:
            updates.append(erratum['advisory_name'] + ' ')
        return updates
    else:
        return None    

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger()
    
    cfg = config()

    while True:
        # Open database connection
        try:
            conn = psycopg2.connect(database=cfg['DATABASE_NAME'],
                                    user=cfg['DATABASE_USER'],
                                    password=cfg['DATABASE_PASSWORD'],
                                    host=cfg['DATABASE_HOST'],
                                    port=cfg['DATABASE_PORT'])
        except:
            logging.debug("Failed to connect to database.")
            continue

        # Get a list of every server
        try:
            cur = conn.cursor()
            cur.execute("SELECT * from autopatch_server")
            servers = cur.fetchmall()
            cur.close()
        except:
            logging.debug("Failed to retrieve server list.")
            continue

        # Loop through servers and if they have a satellite id, check their
        # errata upstream to determine if they are up to date or not.
        for server in servers:
            logging.debug("Running errata checks for {server}".format(server=server[1]))
            if server[6] == 0: # Server doesn't have a satellite id.
                satid = getSatelliteId(server[1])
                logging.debug("Got satid {satid} for {server}".format(satid=satid, server=server[1]))
                if satid != None: # We were able to obtain a satellite id.
                    # Update satellite id
                    try:
                        cur = conn.cursor()
                        cur.execute("UPDATE autopatch_server set satid={satid} where id={sid}".format(satid=satid, sid=server[0]))
                        conn.commit()
                        cur.close()
                    except:
                        conn.rollback()
                        logging.debug("Failed to update satid for {server}".format(server=server[1]))

            updates = None
            if server[6] != 0:
                updates = getErrataForSatId(server[6])
            elif satid != None:
                updates = getErrataForSatId(satid)

            if updates != None:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT * from autopatch_errata")
                    rows = cur.fetchall()
                    cur.close()
                except:
                    continue

                try:
                    cur = conn.cursor()
                    cur.execute("UPDATE autopatch_server set updates=%s where id=%s", (updates, server[0]))
                    conn.commit()
                    cur.close()
                except:
                    logging.debug("Failed to update updates for {server}".format(server=server[1]))
                    conn.rollback()

                RHEA = rows[0][1]
                RHSA = rows[0][2]
                RHBA = rows[0][3]

                needed_updates = desiredErrata(updates, RHEA, RHSA, RHBA)
                if needed_updates:
                    try:
                        cur = conn.cursor()
                        cur.execute("UPDATE autopatch_server set plerrata=%s where id=%s", (needed_updates, server[0]))
                        conn.commit()
                        cur.close()
                    except:
                        conn.rollback()
                        logging.debug("Failed to update errata for {server}".format(server=server[1]))

                    try:
                        cur = conn.cursor()
                        cur.execute("UPDATE autopatch_server set uptodate=FALSE where id={sid}".format(sid=server[0]))
                        conn.commit()
                        cur.close()
                    except:
                        conn.rollback()
                        logging.debug("Failed to update uptodate for {server}".format(server=server[1]))
                else:
                    try:
                        cur = conn.cursor()
                        cur.execute("UPDATE autopatch_server set uptodate=TRUE where id={sid}".format(sid=server[0]))
                        conn.commit()
                        cur.close()
                    except:
                        conn.rollback()
                        logging.debug("Failed to update uptodate for {server}".format(server=server[1]))
        conn.close()
        logger.info("Completed server loop. Sleeping for 3 hours.")
        time.sleep(10800)
