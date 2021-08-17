import json
import requests
import time
import random


def load_movies() -> list:
    with open("../../datasets/movies/movies.json", encoding="UTF-8") as file:
        return json.load(file)


def list_all(item: dict):
    item = item.copy()
    for key in item.keys():
        if isinstance(item[key], list):
            continue

        item[key] = [item[key]]

    return item


def convert_for_lnx(data):
    return list(map(list_all, data))


def chunks(data: list):
    return zip(*[iter(data)]*2500)


def get_random_words(data: list, k=5000) -> list:
    titles = []
    for item in data:
        titles.extend(item['title'].split(" "))

    searches = []
    for _ in range(k):
        searches.append(" ".join(random.choices(titles, k=random.randint(1, 10))))

    return searches


def load_lnx():
    data = load_movies()
    movies = convert_for_lnx(data)

    r = requests.delete("http://127.0.0.1:8000/indexes/movies/documents/clear")
    print(r.json())
    r.raise_for_status()

    start = time.perf_counter()
    for chunk in chunks(movies):
        r = requests.post("http://127.0.0.1:8000/indexes/movies/documents", json=chunk)
        r.raise_for_status()

    requests.post("http://127.0.0.1:8000/indexes/movies/commit")
    stop = time.perf_counter() - start

    print(f"finished loading data to lnx, took: {stop * 1000}ms overall")

    session = requests.Session()
    samples = get_random_words(data)

    start = time.perf_counter()
    for sample in samples:
        r = session.get("http://127.0.0.1:8000/indexes/movies/search", params={"query": sample})
        r.raise_for_status()
    stop = time.perf_counter() - start
    print(f"Samples took: {stop * 1000}ms overall, with {(stop * 1000) / len(samples)}ms per iteration")


def load_meilisearch():
    movies = load_movies()

    r = requests.delete("http://127.0.0.1:7700/indexes/movies/documents")
    print(r.json())
    r.raise_for_status()

    start = time.perf_counter()
    for chunk in chunks(movies):
        r = requests.post("http://127.0.0.1:7700/indexes/movies/documents", json=chunk)
        r.raise_for_status()

    stop = time.perf_counter() - start
    print(f"finished loading data to meili, took: {stop * 1000}ms overall")

    input("wait for meili")

    session = requests.Session()
    samples = get_random_words(movies)

    start = time.perf_counter()
    for sample in samples:
        r = session.post("http://127.0.0.1:7700/indexes/movies/search", json={"q": sample})
        r.raise_for_status()
    stop = time.perf_counter() - start
    print(f"Samples took: {stop * 1000}ms overall, with {(stop * 1000) / len(samples)}ms per iteration")


def start_loading():
    choice = input("load for meilisearch or lnx?\n>>> ")

    if choice.lower() not in ("lnx", "meilisearch"):
        print("invalid choice options: 'meilisearch' or 'lnx'")
        return

    if choice == "lnx":
        load_lnx()
    else:
        load_meilisearch()


if __name__ == '__main__':
    start_loading()

