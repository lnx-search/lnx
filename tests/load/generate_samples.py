import json
import random


def load_movies() -> list:
    with open("../../datasets/reference/movies/movies.json", encoding="UTF-8") as file:
        return json.load(file)


def get_random_words(data: list, k=5000) -> list:
    titles = []
    for item in data:
        titles.extend(item['title'].split(" "))

    searches = []
    for _ in range(k):
        searches.append(" ".join(random.choices(titles, k=random.randint(1, 10))))

    return searches
