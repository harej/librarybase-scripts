import requests
from edit_queue import EditQueue
from site_credentials import *

class CitationGrapher:
    def __init__(self, source, url_pattern, write_thread_count=2):
        self.source = source
        self.url_pattern = url_pattern
        self.write_thread_count= write_thread_count

        self.eq = EditQueue(
            source=self.source,
            url_pattern=self.url_pattern,
            write_thread_count=self.write_thread_count,
            append_value=['P2860'],
            good_refs=[{'P248': None, 'P813': None, 'P854': None}],
            edit_summary='Updating citation graph')

    def get_entitydata(self, manifest):
        packages = [manifest[x:x+50] for x in range(0, len(manifest), 50)]

        for package in packages:
            mapping = {x[0]: x for x in package}
            url = 'https://www.wikidata.org/w/api.php?format=json&action=wbgetentities&ids='
            for wikidata_id in mapping.keys():
                url += wikidata_id + '|'
            url = url[:-1]  # remove trailing pipe

            r = requests.get(url).json()["entities"]

            for wikidata_id, blob in r.items():
                # (wikidata_item, pmcid, cites, retrieve_date)
                yield wikidata_id, blob, mapping[wikidata_id][1], mapping[wikidata_id][2], mapping[wikidata_id][3]

    def process_manifest(self, manifest):
        # Screen against current values on Wikidata
        for relevant_item, blob, relevant_external_id, raw_cites, retrieve_date in self.get_entitydata(manifest):

            cites = []

            # Creating a convenient data object for keeping track of existing
            # 'cites' claims and their references on a given Wikidata item
            references = {}
            if 'claims' in blob:
                if 'P2860' in blob['claims']:
                    extant_claims = blob['claims']['P2860']
                    for claim in extant_claims:
                        if 'mainsnak' in claim:
                            if 'datavalue' in claim['mainsnak']:
                                extant_cited_item = claim['mainsnak']['datavalue']['value']['id']
                                    
                                if 'references' in claim:
                                    for reference in claim['references']:
                                        snaks = {}
                                        for prop_nr, values in reference['snaks'].items():
                                            snaks[prop_nr] = [v['datavalue'] for v in values]

                                        references[extant_cited_item] = snaks

            # Don't generate a statement if the statement already exists and
            # features a fully filled out citation. *Do* otherwise generate
            # a statement, even if the statement exists, if it has a lousy,
            # not-filled-out citation.
            for cited_item in raw_cites:
                generate_statement = True
                if cited_item in references:
                    if 'P248' in references[cited_item] \
                    and 'P813' in references[cited_item] \
                    and 'P854' in references[cited_item]:
                        for x in references[cited_item]['P248']:
                            # stated in: PubMed Central or Crossref
                            if x['value']['id'] in ['Q5188229', 'Q229883']:
                                generate_statement = False

                if generate_statement is True:
                    cites.append(cited_item)

            if len(cites) > 0:
                self.eq.post(relevant_item, relevant_external_id, retrieve_date, cites)
