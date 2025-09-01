from elastic_search import ElasticSearch
from loader import Loader

class Manager:
    def __init__(self):
        self.tweets_list = None
        self.weapons_list = None

        self.load = Loader()
        self.es = ElasticSearch("http://localhost:9200")

    def load_data(self):
        try:
            self.tweets = self.load.get_tweets_list()
            self.weapons = self.load.get_weapons_list()
        except FileNotFoundError as e:
            print("*" * i for i in range(10))
            print(f"I'm sorry, i can't find the file{e.filename}"
                  f"Now the program will crash.")
            print("*" * i for i in range(10))
            raise (e)
        except Exception as e:
            print("*" * i for i in range(10))
            print("Now the program will crash.")
            print("*" * i for i in range(10))
            raise (e)


    def search(self):
        self.es.load_tweets_to_elastic(self.tweets_list, self.weapons_list)
        self.es.add_emotion()
        self.es.match_weapon_to_doc()
        self.es.filter_index()

    def endpoint(self):
        pass