import requests

def main():
    prefix = "http://www.wikidata.org/entity/"
    wdqs = "https://query.wikidata.org/sparql?format=json&query="

    dng_query = "select%20%3Fc%20where%20%7B%20%3Fi%20wdt%3AP3181%20%3Fc%20%7D"
    result = requests.get(wdqs + dng_query).json()["results"]["bindings"]
    do_not_generate = []
    for x in result:
        ocid = x["c"]["value"]
        if ocid not in do_not_generate:
            do_not_generate.append(ocid)

    doi_query = "SELECT%20%3Fi%20%3Fc%20WHERE%20%7B%20%3Fi%20wdt%3AP356%20%3Fc%20.%20%7D"
    result = requests.get(wdqs + doi_query).json()["results"]["bindings"]
    doi_to_wikidata = {x["c"]["value"]: x["i"]["value"].replace(prefix, "") for x in result}

    pmid_query = "SELECT%20%3Fi%20%3Fc%20WHERE%20%7B%20%3Fi%20wdt%3AP698%20%3Fc%20.%20%7D"
    result = requests.get(wdqs + pmid_query).json()["results"]["bindings"]
    pmid_to_wikidata = {x["c"]["value"]: x["i"]["value"].replace(prefix, "") for x in result}

    pmcid_query = "SELECT%20%3Fi%20%3Fc%20WHERE%20%7B%20%3Fi%20wdt%3AP932%20%3Fc%20.%20%7D"
    result = requests.get(wdqs + pmcid_query).json()["results"]["bindings"]
    pmcid_to_wikidata = {"PMC" + x["c"]["value"]: x["i"]["value"].replace(prefix, "") for x in result}

    issn_query = "SELECT%20%3Fi%20%3Fc%20WHERE%20%7B%20%3Fi%20wdt%3AP236%20%3Fc%20.%20%7D"
    result = requests.get(wdqs + issn_query).json()["results"]["bindings"]
    issn_to_wikidata = {x["c"]["value"]: x["i"]["value"].replace(prefix, "") for x in result}

    increment = 10000  # D.R.Y.!
    lower_bound = 0
    upper_bound = increment + 1
    fun_counter = 0

    output = []
    while True:  # There is a condition that will break this.
        fun_counter += 1
        print("Iteration no. " + str(fun_counter))

        occ_query = "http://opencitations.net/sparql?format=json&query=PREFIX+d%3A+%3Chttp%3A%2F%2Fpurl.org%2Fspar%2Fdatacite%2F%3E%0APREFIX+i%3A+%3Chttp%3A%2F%2Fwww.essepuntato.it%2F2010%2F06%2Fliteralreification%2F%3E%0APREFIX+xsd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0ASELECT+%3Fa+%3Fas+%3Fai+WHERE+%7B%0A%3Fa+d%3AhasIdentifier+%3Ft+.%0A%3Ft+d%3AusesIdentifierScheme+%3Fas+%3B+i%3AhasLiteralValue+%3Fai+.%0AFILTER+NOT+EXISTS+%7B%0A%3Ft+d%3AusesIdentifierScheme+d%3Aurl+.%0A%7D%0AFILTER(xsd%3Ainteger(REPLACE(str(%3Fa)%2C+%22https%3A%2F%2Fw3id.org%2Foc%2Fcorpus%2Fbr%2F%22%2C+%22%22))+%3E+{0}+%26%26%0Axsd%3Ainteger(REPLACE(str(%3Fa)%2C+%22https%3A%2F%2Fw3id.org%2Foc%2Fcorpus%2Fbr%2F%22%2C+%22%22))+%3C+{1})%0A%7D%0AORDER+BY+%3Fa"
        # Hard to tell because the URL is so fucking long,
        # but this is actually meant to be encoded with the .format() method.
        # {0} is the lower bound and {1} is the upper bound. e.g occ_query.format(str(0), str(100001))

        occ_blob = requests.get(occ_query.format(str(lower_bound), str(upper_bound))).json()
        lower_bound += increment
        upper_bound += increment

        if "results" in occ_blob:
            if "bindings" in occ_blob["results"]:
                if len(occ_blob["results"]["bindings"]) == 0:
                    break  # HERE IT IS!
                else:
                    for binding in occ_blob["results"]["bindings"]:
                        a_value = binding["a"]["value"].replace("https://www.w3id.org/oc/corpus/br/", "")
                        a_value = a_value.replace("http://opencitations.net/corpus/br/", "")
                        a_scheme = binding["as"]["value"]
                        a_identifier = binding["ai"]["value"]

                        if a_value in do_not_generate:
                            continue

                        output_string = None

                        if a_scheme == "http://purl.org/spar/datacite/pmid":
                            if a_identifier in pmid_to_wikidata:
                                relevant_item = pmid_to_wikidata[a_identifier]
                                output_string = relevant_item + "\tP3181\t\"" + a_value + "\"\tS248\tQ26382154"
                        elif a_scheme == "http://purl.org/spar/datacite/pmcid":
                            if a_identifier in pmcid_to_wikidata:
                                relevant_item = pmcid_to_wikidata[a_identifier]
                                output_string = relevant_item + "\tP3181\t\"" + a_value + "\"\tS248\tQ26382154"
                        elif a_scheme == "http://purl.org/spar/datacite/doi":
                            if a_identifier in doi_to_wikidata:
                                relevant_item = doi_to_wikidata[a_identifier]
                                output_string = relevant_item + "\tP3181\t\"" + a_value + "\"\tS248\tQ26382154"
                        elif a_scheme == "http://purl.org/spar/datacite/issn":
                            if a_identifier in issn_to_wikidata:
                                relevant_item = issn_to_wikidata[a_identifier]
                                output_string = relevant_item + "\tP3181\t\"" + a_value + "\"\tS248\tQ26382154"
                        else:
                            continue

                        if output_string != None:
                            if output_string not in output:
                                output.append(output_string)
            else:
                raise Exception
        else:
            raise Exception

    for output_string in output:
        print(output_string)

if __name__ == '__main__':
    main()
