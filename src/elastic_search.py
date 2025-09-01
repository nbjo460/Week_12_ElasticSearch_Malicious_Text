from elasticsearch import Elasticsearch, helpers
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

class ElasticSearch:
    def __init__(self, url):
        self.es = self._set_elastic(url)
        self.TWEETS_INDEX = "tweets"
        self.WEAPONS_INDEX = "weapons"

        self.EMOTION = "emotion"
        self.WEAPON = "weapon"



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
        then add emotion each dict
        then update the docs
        :return:
        """
        analyze = SentimentIntensityAnalyzer()
        nltk.download('vader_lexicon', download_dir=".")

        def find_emotional_status_text():
            """
            Calculate an emotional ttype of a text.
            By using external Module.
            The emotion can be one of them: negative, positive, neutral.
            :return:
            """

            def absolute_emotion(_emotion_compound):
                """
                Receive a compound of emotients, and return an emotion.
                :param _emotion_compound:
                :return: str
                """

                if 0.5 < _emotion_compound <= 1:
                    return "positive"
                elif _emotion_compound >= -0.5:
                    return "neutral"
                else:
                    return "negative"

            for i in range(len(tweets)):
                emotion_index = analyze.polarity_scores(tweets[i]["_source"]["text"])
                tweets[i]["_source"][self.EMOTION] = absolute_emotion(emotion_index["compound"])
            return tweets

        tweets = self.get_index_docs(self.TWEETS_INDEX)
        documents = find_emotional_status_text()
        self._update_doc_wit_new_parameter(documents, self.TWEETS_INDEX, self.EMOTION)

    def _update_doc_wit_new_parameter(self, _documents, _index, _new_parameter):
        actions = [
            {
                "_op_type": "update",
                "_index" : _index,
                "_id":doc["_id"],
                "doc":{_new_parameter : doc["_source"][_new_parameter]},
            }
            for doc in _documents
        ]
        helpers.bulk(self.es, actions)
        print("Updated succeed")

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

    def get_index_docs(self, index : str) -> list:
        result = []
        docs = helpers.scan(self.es, index=index)
        for doc in docs:
            source = doc
            source["_id"] = doc["_id"]
            result.append(source)
        return result

    def get_docs_with_greater_then_two_weapons(self):
        pass
    @staticmethod
    def _set_elastic(_url):
        es = Elasticsearch(_url)
        if not es.ping():
            raise "NO PING, WRONG URL, BAD CONNECTION"
        return es
