import html
import requests
import threading

class AskPubMed(threading.Thread):
    def __init__ (self, threadID, name, packages):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.packages = packages

    def run(self):
        esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&retmode=json&tool=wikidata_worker&email=jamesmhare@gmail.com&id="
        idconv_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="
        months = {
                   "Jan": "01",
                   "Feb": "02",
                   "Mar": "03",
                   "Apr": "04",
                   "May": "05",
                   "Jun": "06",
                   "Jul": "07",
                   "Aug": "08",
                   "Sep": "09",
                   "Oct": "10",
                   "Nov": "11",
                   "Dec": "12"
                 }

        for package in self.packages:
            bunch_of_numbers = ""
            for pmid in package:
                bunch_of_numbers += pmid + ","
            bunch_of_numbers = bunch_of_numbers[:-1]  # Remove trailing comma

            summary_retriever = requests.get(esummary_url + bunch_of_numbers)

            if summary_retriever.status_code != 200:
                continue

            # Now processing the bibliographic metadata from our summary retriever query...
            summary_retriever_json = summary_retriever.json()
            if "result" in summary_retriever_json:
                for _, pmid_blob in summary_retriever_json["result"].items():
                    if _ == "uids":
                        continue

                    pmid = pmid_blob["uid"]

                    # First: The basics
                    output_string  = "CREATE\n"
                    output_string += "LAST\tP698\t\"" + pmid + "\"\tS248\tQ180686\n"
                    output_string += "LAST\tP31\tQ13442814\tS248\tQ180686\n"
                    output_string += "LAST\tDen\t\"" + "scientific article" + "\"\n"

                    # Are there other IDs we can add?
                    doi = None  # if there is a DOI, this value will be overridden
                    if "articleids" in pmid_blob:
                        for identifier in pmid_blob["articleids"]:
                            if identifier["idtype"] == "doi":
                                doi = identifier["value"]  # We want the DOI for later
                                output_string += "LAST\tP356\t\"" + identifier["value"] + "\"\tS248\tQ180686\n"
                            elif identifier["idtype"] == "pmc":
                                pmcid = identifier["value"].replace("PMC", "")
                                output_string += "LAST\tP932\t\"" + pmcid + "\"\tS248\tQ180686\n"

                    # Title
                    if "title" in pmid_blob:
                        t = html.unescape(pmid_blob["title"])
                        if t != "":
                            if t[-1:] == ".":
                                t = t[:-1]
                            if t[0] == "[" and t[-1:] == "]":
                                t = t[1:-1]
                            output_string += "LAST\tLen\t\"" + t + "\"\n"
                            output_string += "LAST\tP1476\ten:\"" + t + "\"\tS248\tQ180686\n"

                    # Publication date
                    if "pubdate" in pmid_blob:
                        pubdate = None
                        pubdate_raw = pmid_blob["pubdate"].split(" ")  # 2016 Aug 1
                        if len(pubdate_raw) > 1:
                            if pubdate_raw[1] in months:
                                m = months[pubdate_raw[1]]
                            else:
                                m = '00'
                        if len(pubdate_raw) == 3:  # Precision to the day
                            pubdate = "+{0}-{1}-{2}T00:00:00Z".format(pubdate_raw[0], m, pubdate_raw[2].zfill(2))
                            precision = 11
                        elif len(pubdate_raw) == 2:  # Precision to the month
                            pubdate = "+{0}-{1}-00T00:00:00Z".format(pubdate_raw[0], m)
                            precision = 10
                        elif len(pubdate_raw) == 1:  # Precision to the year
                            pubdate = "+{0}-00-00T00:00:00Z/9".format(pubdate_raw[0])

                        if pubdate != None:
                            pubdate = pubdate.replace("-00-00T00:00:00Z/10", "-00-00T00:00:00Z/9")
                            output_string += "LAST\tP577\t" + pubdate + "\tS248\tQ180686\n"

                    # Published in
                    if "issn" in pmid_blob:
                        issn_query_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fissn%20where%20%7B%20%3Fi%20wdt%3AP236%20%22{0}%22%20%7D"
                        issn_query = requests.get(issn_query_url.format(pmid_blob["issn"])).json()
                        issn_results = issn_query["results"]["bindings"]
                        if len(issn_results) == 1:  # We want no ambiguity here
                            journal = issn_results[0]["i"]["value"].replace("http://www.wikidata.org/entity/", "")
                            output_string += "LAST\tP1433\t" + journal + "\tS248\tQ180686\n"

                    # Volume
                    if "volume" in pmid_blob:
                        if pmid_blob["volume"] != "":
                            output_string += "LAST\tP478\t\"" + pmid_blob["volume"] + "\"\tS248\tQ180686\n"

                    # Issue
                    if "issue" in pmid_blob:
                        if pmid_blob["issue"] != "":
                            output_string += "LAST\tP433\t\"" + pmid_blob["issue"] + "\"\tS248\tQ180686\n"

                    # Pages
                    if "pages" in pmid_blob:
                        if pmid_blob["pages"] != "":
                            output_string += "LAST\tP304\t\"" + pmid_blob["pages"] + "\"\tS248\tQ180686\n" 

                    # Original language
                    if "lang" in pmid_blob:
                        for langcode in pmid_blob["lang"]:
                            if langcode == "eng":
                                output_string += "LAST\tP364\tQ1860\tS248\tQ180686\n"
                                break

                    # Authors
                    authors_not_done = True  # set to False if authors are successfully extracted via Crossref
                    if doi != None:
                        try:
                            crossref = requests.get("https://dx.doi.org/" + doi, headers={"Accept": "application/json"})
                        except:
                            pass
                        if crossref.status_code == 200:
                            try:
                                crossref_json = crossref.json()
                                if "author" in crossref_json:
                                    authors_not_done = False
                                    author_counter = 0
                                    for author in crossref_json["author"]:
                                        author_counter += 1
                                        a = ""
                                        if "family" in author:
                                            a = author["family"]
                                        if "given" in author:
                                            a = author["given"] + " " + a
                                        output_string += "LAST\tP2093\t\"" + a + "\"\tP1545\t\"" + str(author_counter) + "\"\tS248\tQ5188229\n"
                            except:
                                pass

                    if "authors" in pmid_blob and authors_not_done == True:
                        author_counter = 0
                        for author in pmid_blob["authors"]:
                            if author["authtype"] == "Author":
                                author_counter += 1
                                output_string += "LAST\tP2093\t\"" + author["name"] + "\"\tP1545\t\"" + str(author_counter) + "\"\tS248\tQ180686\n"

                    output_string = output_string[:-1]
                    print(output_string)


def main(seed_url):
    seed = requests.get(seed_url).json()
    full_pmid_list = [x for x in seed["esearchresult"]["idlist"]]

    wikidata = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fp%20where%20%7B%20%3Fi%20wdt%3AP698%20%3Fp%20%7D").json()
    wikidata_pmid_list = [x["p"]["value"] for x in wikidata["results"]["bindings"]]

    pmid_list = list(set(full_pmid_list) - set(wikidata_pmid_list))

    # A list of 200 IDs makes a package. These collectively are the "packages".
    # The package of all these packages is the "Package of Packages".
    packages = [pmid_list[x:x+200] for x in range(0, len(pmid_list), 200)]
    package_of_packages = [packages[x:x+250] for x in range(0, len(packages), 250)]

    thread_counter = 0
    for packages in package_of_packages:
        thread = AskPubMed(thread_counter, "thread-" + str(thread_counter), packages)
        thread_counter += 1
        thread.start()

if __name__ == "__main__":
    main("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?retmode=json&email=jamesmhare@gmail.com&tool=wikidata_worker&db=pubmed&term=review[filter]%20free%20full%20text[filter]&reldate=735&datetype=edat&retmax=250000")