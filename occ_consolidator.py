import json
import os

dirs = ['be', 'id', 're']
occ = {x: {} for x in dirs}

def mapper(record, field_name, corresponding_dirr):
    if field_name not in record:
        return []

    subrecord = record[field_name]
    if type(subrecord) is list:
        new = []
        for subiri in subrecord:
            to_append = occ[corresponding_dirr][int(subiri[4:])]
            del occ[corresponding_dirr][int(subiri[4:])]
            new.append(to_append)
        return new

    elif type(subrecord) is str:
        to_return = occ[corresponding_dirr][int(subrecord[4:])]
        del occ[corresponding_dirr][int(subrecord[4:])]
        return [to_return]

    else:
        raise Exception('Neither a list nor a string???')

def dir_crawler(dirlist):
    for dirr in dirlist:
        for root, subdir, files in os.walk('../OpenCitations/' + dirr + '/'):
            for file in files:
                filepath = os.path.join(root, file)
                if filepath.endswith('.json') and not file.startswith('.'):
                    print('Processing: ' + filepath)
                    with open(filepath) as f:
                        blob = json.load(f)
                        for record in blob['@graph']:
                            yield dirr, record

def main():
    result = {}

    for dirr, record in dir_crawler(dirs):
        iri = int(record['iri'][4:])  # lop off prefix
        occ[dirr][iri] = record

    # Combine ra and id
    print('Combining ra and id')
    occ['ra'] = {}
    for dirr, record in dir_crawler(['ra']):
        iri = int(record['iri'][4:])
        occ['ra'][iri] = record
        if 'identifier' in record:
            identifier_ref = int(record['identifier'][4:])
            occ['ra'][iri]['identifier'] = occ['id'][identifier_ref]
            del occ['id'][identifier_ref]

    # Combine ar and ra
    occ['ar'] = {}
    print('Combining ar and ra')
    for dirr, record in dir_crawler(['ar']):
        iri = int(record['iri'][4:])
        occ['ar'][iri] = record
        role_of_ref = int(record['role_of'][4:])
        occ['ar'][iri]['role_of'] = occ['ra'][role_of_ref]
    del occ['ra']

    for dirr, record in dir_crawler(['br']):
        iri = int(record['iri'][4:])
        print("Processing: " + str(iri))
        result[iri] = record
        result[iri]['contributor'] = mapper(record, 'contributor', 'ar')
        result[iri]['reference'] = mapper(record, 'reference', 'be')
        result[iri]['identifier'] = mapper(record, 'identifier', 'id')
        result[iri]['format'] = mapper(record, 'format', 're')

    del occ

    print('Saving')
    with open('consolidated_occ.json', 'w') as f:
        json.dump(result, f, indent=4)

if __name__ == '__main__':
    main()
