import requests

def main():
    esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&retmode=json&tool=wikidata_worker&email=jamesmhare@gmail.com&id="

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

    seed_url = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP577%20%3Fdummy0%20%7D%0A%20%20FILTER%28%21bound%28%3Fdummy0%29%29%0A%7D"
    seed = requests.get(seed_url).json()["results"]["bindings"]

    pmid_to_wikidata = {}
    pmid_list = []

    for x in seed:
    	pmid_to_wikidata[x["pmid"]["value"]] = x["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    	pmid_list.append(x["pmid"]["value"])

    packages = [pmid_list[x:x+200] for x in range(0, len(pmid_list), 200)]

    for package in packages:
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

                # Publication date
                if "pubdate" in pmid_blob:
                    pubdate = None
                    pubdate_raw = pmid_blob["pubdate"].split(" ")  # 2016 Aug 1
                    if len(pubdate_raw) > 1:
                        if pubdate_raw[1] in months:
                            m = months[pubdate_raw[1]]
                        else:
                            m = "00"                            
                    if len(pubdate_raw) == 3:  # Precision to the day
                        if pubdate_raw[2].zfill(2) in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]:
                            pubdate = "+{0}-{1}-{2}T00:00:00Z/11".format(pubdate_raw[0], m, pubdate_raw[2].zfill(2))
                        else:
                            pubdate = "+{0}-{1}-00T00:00:00Z/10".format(pubdate_raw[0], m)
                    elif len(pubdate_raw) == 2:  # Precision to the month
                        pubdate = "+{0}-{1}-00T00:00:00Z/10".format(pubdate_raw[0], m)
                    elif len(pubdate_raw) == 1:  # Precision to the year
                        pubdate = "+{0}-00-00T00:00:00Z/9".format(pubdate_raw[0])

                    if pubdate != None:
                        pubdate = pubdate.replace("-00-00T00:00:00Z/10", "-00-00T00:00:00Z/9")
                        print(pmid_to_wikidata[pmid] + "\tP577\t" + pubdate + "\tS248\tQ180686")

if __name__ == '__main__':
	main()
