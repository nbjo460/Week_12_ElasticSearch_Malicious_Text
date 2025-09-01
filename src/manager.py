from elastic_search import ElasticSearch
from loader import Loader
import endpoint
import uvicorn

class Manager:
    def __init__(self):
        self.tweets_list = None
        self.weapons_list = None

        self.load = Loader()
        self.es = ElasticSearch("http://localhost:9200")

    def load_data(self):
        try:
            self.tweets_list = self.load.get_tweets_list()
            self.weapons_list = self.load.get_weapons_list()
        except FileNotFoundError as e:
            print("".join("*" * i for i in range(10)))
            print(f"I'm sorry, i can't find the file{e.filename}"
                  f"Now the program will crash.")
            print("".join("*" * i for i in range(10)))
            raise e
        except Exception as e:
            print("".join("*" * i for i in range(10)))
            print("Now the program will crash.")
            print("".join("*" * i for i in range(10)))
            raise e

    def search(self):
        self.es.load_data(self.tweets_list, self.weapons_list)
        self.es.add_emotion()
        self.es.match_weapon_to_doc()
        self.es.filter_index()

    @staticmethod
    def endpoint():
        uvicorn.run(endpoint.app, host="0.0.0.0", port = 8000)