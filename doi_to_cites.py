import arrow
import codeswitch
import json
import threading
import time
from bz2 import BZ2File as bzopen
from edit_queue import EditQueue
from citation_grapher import CitationGrapher

print('Setting up globals')

WRITE_THREAD_COUNT = 2
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

def update_graph(doi, wd_item, cites):
    CG = CitationGrapher(eq)
    CG.process_manifest({wd_item: (doi, tuple(cites), '+2018-01-21T00:00:00Z')})

def main():
    with bzopen('assets/crossref_references.jsonl.bz2', 'r') as f:
        for line in f:
            print('. ', end='', flush=True)
            while threading.active_count() >= THREAD_LIMIT:
                time.sleep(0.25)

            mapping = json.loads(line)
            doi_x = list(mapping.keys())[0]
            lookup = [doi_x]

            for doi_y in mapping[doi_x]:
                lookup.append(doi_y)

            lookup = codeswitch.doi_to_wikidata(lookup)

            if lookup[0] is None:
                continue

            cites = []
            for wd_y in lookup:
                if wd_y is None:
                    continue
                if wd_y == lookup[0]:
                    continue
                cites.append(wd_y)

            if len(cites) > 0:
                t = threading.Thread(target=update_graph, args =(doi_x, lookup[0], cites))
                t.daemon = True
                t.start()

if __name__ == '__main__':
    main()
