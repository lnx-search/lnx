import json


def load_movies() -> list:
    with open("../../datasets/reference/movies/movies.json", encoding="UTF-8") as file:
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


if __name__ == '__main__':
    movies = load_movies()
    movies = convert_for_lnx(movies)

    with open("../../datasets/converted/lnx_movies.json", "w+", encoding="UTF-8") as file:
        json.dump(movies, file)
