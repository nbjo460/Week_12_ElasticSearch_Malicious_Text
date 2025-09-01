
class Loader:
    def __init__(self, _weapons_list :str = "", _tweets : str = ""):
        self.FILES_PATH = "../data/"
        exists_files = self._validate_files_names(self.FILES_PATH ,_weapons_list, _tweets)

        self.WEAPONS_LIST_FILE_NAME = "weapon_list.txt" if not exists_files["weapons"] else exists_files["weapons"]
        self.TWEETS_FILE_NAME = "tweets_injected 3.csv" if not exists_files["tweets"] else exists_files["tweets"]

    def get_weapons_list(self) -> list:
        """
        load file, return list of weapons if exists, else throw new exception.
        :return: list
        """
        try:
            weapons = []
            with open(self.FILES_PATH + self.WEAPONS_LIST_FILE_NAME, "r") as file:
                for line in file:
                    weapons.append(line)
            return weapons
        except Exception as e:
            print("Weapons file is missing!\n" + e)

    def get_tweets_list(self) -> list:
        try:
            tweets = []
            with open(self.FILES_PATH + self.TWEETS_FILE_NAME, "r") as file:
                for line in file:
                    tweets.append(line)
            return tweets
        except Exception as e:
            print("Weapon file is missing!\n" + e)

    @staticmethod
    def _validate_files_names(_files_path ,_weapons_list_file_name, _tweets_file_name):
        import os.path as path
        exists_files = {}
        weapons_exists = path.isfile(_files_path+_weapons_list_file_name)
        tweets_exists = path.isfile(_files_path+_tweets_file_name)
        exists_files["weapons"] = weapons_exists
        exists_files["tweets"] = tweets_exists
        return exists_files