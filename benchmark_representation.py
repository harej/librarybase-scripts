import redis
import ast

REDIS = redis.Redis(host='127.0.0.1', port=6379)

amt = 100000

for x in range(0, amt):
	REDIS.set('benchmark__' + str(x), {'doi': '10.100/abc', 'pmc': '12345', 'provenance': {'doi': {'pmc': {'url': 'https://example.com', 'date': 'YYYYMMDD'}}}})

for x in range(0, amt):
	blah = REDIS.get('benchmark__' + str(x)).decode('UTF-8')
	blah = ast.literal_eval(blah)

for x in range(0, amt):
	REDIS.delete('benchmark__' + str(x))
