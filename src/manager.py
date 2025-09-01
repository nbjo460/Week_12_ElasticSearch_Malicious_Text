from elastic_search import ElasticSearch as es
class Manager:
    def __init__(self):
        pass
    def search(self):
        es.load_tweets_to_elastic()
        es.add_emotion()
        es.match_weapon_to_doc()
        es.filter_index()

    def endpoint(self):
        pass