import arrow
import codeswitch
import json
import threading
import time
from bz2 import BZ2File as bzopen
from edit_queue import EditQueue
from citation_grapher import CitationGrapher

print('Setting up globals')

WRITE_THREAD_COUNT = 3
THREAD_LIMIT = WRITE_THREAD_COUNT + 2

# Go from newest Wikidata QID to oldest?
DESCENDING_ORDER = True

eq = EditQueue(
         source='Q5188229',
         url_pattern='https://api.crossref.org/works/',
         write_thread_count=WRITE_THREAD_COUNT,
         append_value=['P2860'],
         good_refs=[{'P248': None, 'P813': None, 'P854': None}],
         edit_summary='Updating citation graph',
         alt_account=True)

print('Done setting up globals')

def update_graph(doi, cites):
    CG = CitationGrapher(eq)
    CG.process_manifest({codeswitch.doi_to_wikidata(doi): (doi, tuple(cites), '+2018-01-21T00:00:00Z')})
    print('. ', end='', flush=True)

def main():
    with bzopen('assets/crossref_references.jsonl.bz2', 'r') as f:
        for line in f:
            while threading.active_count() >= THREAD_LIMIT:
                time.sleep(0.25)

            mapping = json.loads(line)
            doi_x = list(mapping.keys())[0]
            wd_x = codeswitch.doi_to_wikidata(doi_x)
            if wd_x is None:
                continue

            cites = []

            for doi_y in mapping[doi_x]:
                wd_y = codeswitch.doi_to_wikidata(doi_y)
                if wd_y is None:
                    continue
                if wd_x == wd_y:
                    continue
                cites.append(wd_y)

            if len(cites) > 0:
                update_graph(doi_x, cites)

if __name__ == '__main__':
    main()
