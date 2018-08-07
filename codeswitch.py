import redis
from site_credentials import *

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)

def identifier_generator(wd_prop, list_name, descending_order=True):
    REDIS.delete(list_name)

    identifier_list = REDIS.hgetall(wd_prop + '_to_wikidata')
    identifier_list = [y.decode('utf-8') + '|' + x.decode('utf-8') for x, y in identifier_list.items()]
    identifier_list.sort(reverse=descending_order)
    identifier_list = [x.split('|')[1] for x in identifier_list]

    for entry in identifier_list:
        REDIS.rpush(list_name, entry)

    del identifier_list
    
    while REDIS.llen(list_name) > 0:
        yield REDIS.lpop(list_name)

def hget(keyname, itemname):
    raw = REDIS.hget(keyname, itemname)

    if raw is None:
        return None
    else:
        return raw.decode('utf-8')

def doi_to_wikidata(doi):
    return hget('P356_to_wikidata', doi)

def pmid_to_wikidata(pmid):
    return hget('P698_to_wikidata', pmid)

def pmcid_to_wikidata(pmcid):
    return hget('P932_to_wikidata', pmcid)

def wikidata_to_doi(wikidata):
    return hget('wikidata_to_P356', wikidata)

def wikidata_to_pmid(wikidata):
    return hget('wikidata_to_P698', wikidata)

def wikidata_to_pmcid(wikidata):
    return hget('wikidata_to_P932', wikidata)
