from fastapi import FastAPI
from elastic_search import ElasticSearch

app = FastAPI()

@app.get("/")
def root():
    return {"status":"200"}
@app.get("/download_all")
def download_all():
    search = ElasticSearch("http://localhost:9200")
    result = search.get_index_docs(search.TWEETS_INDEX)
    return result
# @app.get("/")
