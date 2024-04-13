import sys
import csv
import os
from pymongo import MongoClient
import typer

app = typer.Typer()

FILENAME_RES = '../../cdi-data/dumps/resources_uniq.tsv'
FILENAME_FIL = '../../cdi-data/dumps/filenames.csv'

csv.field_size_limit(100000000)

@app.command()
def run():
  f = open(FILENAME_RES, 'r', encoding='utf8')
  reader = csv.DictReader(f, delimiter='\t')
  out = open(FILENAME_FIL, 'w', encoding='utf8')
  writer = csv.writer(out, delimiter=',')
  writer.writerow(['filename', 'ext', 'format', 'mimetype'])
  n = 0
  for row in reader:
      n += 1
      if len(row['url']) > 500: continue
      if row['format'] is not None and len(row['format']) > 500: continue
      if row['mimetype'] is not None and len(row['mimetype']) > 500: continue
      filename = row['url'].strip('"').strip().rsplit('/', 1)[-1].strip().split('?', 1)[0].split('#', 1)[0].split('&', 1)[0].strip().split('%3F', 1)[0].replace('"','').replace("'", '').replace('%20', '_').replace(':', '_').split('\\t', 1)[0]
      ext = filename.rsplit('.', 1)[-1] if filename.find('.') > -1 else ''
      ext = ext.replace('\\', '_').replace('/', '_')
      if len(ext) > 8: 
          ext = ''
      writer.writerow([filename, ext, row['format'], row['mimetype']])
      if n % 100000 == 0: print(n)

  f.close()
  out.close()

if __name__ == "__main__":
  app()