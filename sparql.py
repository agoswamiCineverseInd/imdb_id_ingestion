from SPARQLWrapper import SPARQLWrapper, JSON,XML
from logger import send_log_async
from db import insert_imdb_batch,update_checkpoint

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")


def get_count(year):
     count = 0
     try:
          count_query = f"""SELECT (COUNT(?imdbId) AS ?count) WHERE {{?movie wdt:P31 wd:Q11424;wdt:P577 ?releaseDate; wdt:P345 ?imdbId.                     VALUES ?type {{
                          wd:Q11424       
                          wd:Q5398426     
                          }}FILTER(YEAR(?releaseDate) = {year})}}"""
          sparql.setQuery(count_query)
          sparql.setReturnFormat(JSON)
          result = sparql.query().convert()
          count = int(result["results"]["bindings"][0]["count"]["value"])
     except Exception as e:
          send_log_async("error","sparql.py(get_count)",year,str(e))
          count = 0
     return count

def ingest_imdbIds_by_year(year,last_db_imdb,conn_pool):
     count = get_count(year)
     last_imdb = last_db_imdb
     while True:
          try:
               query = f"""
            SELECT ?imdbId WHERE {{
              ?movie wdt:P31 wd:Q11424;
                     wdt:P577 ?releaseDate;
                     wdt:P345 ?imdbId.
                     VALUES ?type {{
                          wd:Q11424       
                          wd:Q5398426     
                          }}

              FILTER(YEAR(?releaseDate) = {year})
              {"FILTER(?imdbId > \"" + last_imdb + "\")" if last_imdb else ""}
            }}
            ORDER BY ?imdbId
            LIMIT 500
            """
               sparql.setQuery(query)
               sparql.setReturnFormat(JSON)
               result = sparql.query().convert()
               imdb_ids = extract_ids(result)
               if len(imdb_ids)==0:
                    break
               last_imdb=imdb_ids[-1]
               insert_imdb_batch(conn_pool,imdb_ids)
               update_checkpoint(last_imdb,year,conn_pool)
          except Exception as e :
               # something
               send_log_async("error","sparql.py -> ingest_imdbIds_by_yearet_count()",year,str(e))
          finally:
               count-=100
               send_log_async("info","sparql.py -> ingest_imdbIds_by_yearet_count()",year,f"documents left to insert {count}")
               if count<=0:
                    break

def extract_ids(res): 
    return [r.get("imdbId", {}).get("value") for r in res.get("results", {}).get("bindings", []) if r.get("imdbId")] if res else []
