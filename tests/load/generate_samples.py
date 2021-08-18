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


if __name__ == '__main__':
    movies = load_movies()
    random_searches = get_random_words(movies, k=50)

    with open("../../datasets/search_samples/samples.json", "w+", encoding="UTF-8") as file:
        json.dump(random_searches, file)
