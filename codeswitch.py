import redis
from collections import OrderedDict
from site_credentials import *

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)

def get_all_items(wd_prop, descending_order=True):
    identifier_list = hgetall('wikidata_to_' + wd_prop)
    identifier_list = [x + '|' + y for x, y in identifier_list.items()]
    identifier_list.sort(reverse=descending_order)

    return OrderedDict({x.split('|')[0]: x.split('|')[1] for x in identifier_list})

def hgetall(keyname):
    raw = REDIS.hgetall(keyname)
    return {x.decode('utf-8'): y.decode('utf-8') for x, y in raw.items()}

def hget(keyname, itemname):
    if type(itemname) is str:
        raw = REDIS.hget(keyname, itemname)
        if raw is None:
            return None
        else:
            return raw.decode('utf-8')

    else:
        raw = REDIS.hmget(keyname, itemname)
        ret = []
        for result in raw:
            if result is None:
                ret.append(None)
            else:
                ret.append(result.decode('utf-8'))

        return ret

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
