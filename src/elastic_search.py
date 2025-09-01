from elasticsearch import Elasticsearch

class ElasticSearch:
    def __init__(self, url):
        self.es = Elasticsearch(url)
    def load_tweets_to_elastic(self):
        pass
    def add_emotion(self):
        """
        read tweets
        then add emotion each row
        then insert new row
        :return:
        """
    def filter_index(self):
        """
        delete all rows those:
        Not antisemitic
        and not include any weapon,
        and emotion is positive or neutral.
        :return:
        """
    def match_weapon_to_doc(self):
        """
        run over each row, then check what weapon is inside the text.
        add all matches to a list, and add the list to new col.
        :return:
        """
    def get_all_docs(self):
        pass
    def get_docs_with_greater_then_two_weapons(self):
        pass