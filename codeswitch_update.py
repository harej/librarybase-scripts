import arrow
import re
import redis
from bz2 import BZ2File as bzopen
from site_credentials import redis_server, redis_port, redis_key

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)
today = arrow.utcnow().format('YYYYMMDD')

def main():
    # <http://www.wikidata.org/entity/Q47133351> <http://www.wikidata.org/prop/direct/P356> "10.1002/EJP.1050" .
    REGEX = r'^<http:\/\/www\.wikidata\.org\/entity\/(Q\d+)> <http:\/\/www.wikidata.org\/prop\/direct\/(P\d+)> "(.*?)" \.$'
    manifest = ['P356', 'P698', 'P932', 'P2880']
    dump_location = '/public/dumps/public/wikidatawiki/entities/latest-truthy.nt.bz2'
    to_add = {x: [] for x in manifest}

    with bzopen(dump_location, 'r') as f:
        for line in f:
            line = line.decode('utf-8')
            match = re.match(REGEX, line)
            if match is None:
                continue

            wd_item  = match.group(1)
            wd_prop  = match.group(2)
            wd_value = match.group(3)

            if wd_prop in manifest:
                print('Up to', wd_item, end='\r')
                to_add[wd_prop].append((wd_item, wd_value))

                if len(to_add[wd_prop]) >= 10000:
                    print('\nSaving to Redis')

                    wikidata_to_x = {x[0]: x[1] for x in to_add[wd_prop]}
                    x_to_wikidata = {x[1]: x[0] for x in to_add[wd_prop]}

                    REDIS.hmset(
                        '{0}_to_wikidata_{1}'.format(wd_prop, today),
                        x_to_wikidata)
                    REDIS.hmset('wikidata_to_{0}_{1}'.format(wd_prop, today),
                        wikidata_to_x)

                    to_add[wd_prop] = []

    # If there are leftovers
    for wd_prop, tuplelist in to_add.items():
        wikidata_to_x = {x[0]: x[1] for x in tuplelist}
        x_to_wikidata = {x[1]: x[0] for x in tuplelist}

        REDIS.hmset(
            '{0}_to_wikidata_{1}'.format(wd_prop, today),
            x_to_wikidata)
        REDIS.hmset('wikidata_to_{0}_{1}'.format(wd_prop, today),
            wikidata_to_x)

    # Finalize
    for wd_prop in manifest:
        REDIS.rename('{0}_to_wikidata_{1}'.format(wd_prop, today),
            '{0}_to_wikidata'.format(wd_prop))
        REDIS.rename('wikidata_to_{0}_{1}'.format(wd_prop, today),
            'wikidata_to_{0}'.format(wd_prop))

if __name__ == '__main__':
    main()
