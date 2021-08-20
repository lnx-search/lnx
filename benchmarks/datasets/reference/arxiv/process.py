import json
import requests


file = open("./arxiv-metadata-oai-snapshot.json", encoding="UTF-8")

data = []
while True:
    while len(data) < 10_000:
        doc = json.loads(file.readline())

        del doc['journal-ref']
        del doc['doi']
        del doc['report-no']
        del doc['categories']
        del doc['license']
        del doc['versions']
        del doc['authors_parsed']
        del doc['update_date']

        doc['id'] = [doc['id'].strip()]
        doc['submitter'] = [doc['submitter'].strip()] if doc['submitter'] else []
        doc['authors'] = [doc['authors'].strip()] if doc['authors'] else []
        doc['title'] = [doc['title'].strip()] if doc['title'] else []
        doc['comments'] = [doc['comments'].strip()] if doc['comments'] else []
        doc['abstract'] = [doc['abstract'].strip()] if doc['abstract'] else []

        data.append(doc)

    r = requests.post("http://127.0.0.1:8000/indexes/bench/documents", json=data)
    r.raise_for_status()

    r = requests.post("http://127.0.0.1:8000/indexes/bench/commit")
    r.raise_for_status()
    data = []
    print("added 10k docs")



