from elastic_search import ElasticSearch
from loader import Loader

class Manager:
    def __init__(self):
        self.tweets = None
        self.weapons = None

        self.load = Loader()
        self.es = ElasticSearch("http://localhost:9200")

    def load_data(self):
        try:
            self.tweets = self.load.get_tweets_list()
            self.weapons = self.load.get_weapons_list()
        except FileNotFoundError as e:
            print("*" * i for i in range(10))
            print(f"I'm sorry, i can't find the file{e.filename}\n{e}"
                  f"Now the program will crash.")
            print("*" * i for i in range(10))

    def search(self):
        es.load_tweets_to_elastic()
        es.add_emotion()
        es.match_weapon_to_doc()
        es.filter_index()

    def endpoint(self):
        pass