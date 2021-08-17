import json
import requests


def load_movies() -> list:
    with open("./datasets/movies/movies.json") as file:
        return json.load(file)


def list_all(item: dict):
    for key in item.keys():
        if isinstance(item[key], list):
            continue

        item[key] = [item[key]]

    return item


def convert_for_lnx(data):
    return list(map(list_all, data))


def chunks(data: list):
    return zip(*[iter(data)]*2500)


def load_lnx():
    movies = load_movies()
    movies = convert_for_lnx(movies)

    for chunk in chunks(movies):
        requests.post()

    print("finished loading data to lnx")


def load_meilisearch():
    movies = load_movies()
    for chunk in chunks(movies):
        r = requests.post("http://127.0.0.1:7700/indexes/movies/documents", json=chunk)
        r.raise_for_status()

    print("finished loading data to meiliserach")


def start_loading():
    choice = input("load for meilisearch or lnx?\n>>> ")

    if choice.lower() not in ("lnx", "meilisearch"):
        print("invalid choice options: 'meilisearch' or 'lnx'")
        return

    if choice == "lnx":
        load_lnx()
    else:
        load_meilisearch()



