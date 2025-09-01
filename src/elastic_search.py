from elasticsearch import Elasticsearch, helpers

class ElasticSearch:
    def __init__(self, url):
        self.es = self._set_elastic(url)
        self.TWEETS_INDEX = "tweets"
        self.WEAPONS_INDEX = "weapons"



    def load_data(self, _tweets_list, _weapons_list):
        """
                    Load weapons list and tweets data to elastic search.
        :param _tweets_list:
        :param _weapons_list:
        :return:
        """
        self._load_tweets_to_elastic(_tweets_list)
        self._load_weapons_to_elastic(_weapons_list)

    def _load_tweets_to_elastic(self, _tweets_list) -> None:
         """
            Create indexes, then add data.
         :param _tweets_list: list of tweets
         :return: None
         """
         #    add a tweets index if not exists
         if self.es.indices.exists(index=self.TWEETS_INDEX):
            self.es.indices.delete(index=self.TWEETS_INDEX)
         else:
             self.es.indices.create(index=self.TWEETS_INDEX)

         actions = [
            {
            "_index" : self.TWEETS_INDEX,
            "_source" : tweet
            }
            for tweet in _tweets_list]
         helpers.bulk(self.es, actions)
         print(f"{len(_tweets_list)} tweets added")

    def _load_weapons_to_elastic(self, _weapons_list):
        """
           Create indexes, then add data.
        :param _weapons_list: list of tweets
        :return: None
        """
        #    add a weapons index if not exists
        if self.es.indices.exists(index=self.WEAPONS_INDEX):
            self.es.indices.delete(index=self.WEAPONS_INDEX)
        else:
            self.es.indices.create(index=self.WEAPONS_INDEX)
        actions = [
            {
                "_index": self.WEAPONS_INDEX,
                "_source": {"value" : weapon}
            }
            for weapon in _weapons_list]
        helpers.bulk(self.es, actions)
        print(f"{len(_weapons_list)} weapons added")

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
    @staticmethod
    def _set_elastic(_url):
        es = Elasticsearch(_url)
        if not es.ping():
            raise "NO PING, WRONG URL, BAD CONNECTION"
        return es
