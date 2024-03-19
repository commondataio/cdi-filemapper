import sys
import csv
import os
from pymongo import MongoClient
import typer

app = typer.Typer()

def dict2csv(data, headers, filename, limit=None):
  f = open(filename, 'w', encoding='utf8')
  writer = csv.writer(f, delimiter=',')
  output = sorted(data.items(), key=lambda x: x[1], reverse=True)  
  writer.writerows(output)
  f.close()


@app.command()
def run():
  keywords = {}
  topics = {}
  client = MongoClient()
  db = client['cdisearch']
  coll = db['fulldb']
  n = 0
  f = open('resources.tsv', 'w', encoding='utf8')
  writer = csv.writer(f, delimiter='\t')
  writer.writerow(['url','format', 'mimetype'])
  for record in coll.find({'resources' : {"$not": {"$size": 0}}}, {'resources' : 1}):    
    n +=  1
    for r in record['resources']: 
        if 'url' not in r.keys() or r['url'] is None or len(r['url'].strip()) == 0: continue
        url = r['url']
        mimetype = r['mimetype'] if 'mimetype' in r.keys() else ''
        fileformat = r['format'] if 'format' in r.keys() else ''
        writer.writerow([url, fileformat, mimetype])
    if n % 10000 == 0: print(n)

if __name__ == "__main__":
  app()