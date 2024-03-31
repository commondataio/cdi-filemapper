import sys
import csv
import os
import typer
import yaml

app = typer.Typer()

FILENAME_WS = '../data/workset.tsv'

csv.field_size_limit(100000000)

@app.command()
def run():
    data = []
    f = open(FILENAME_WS, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        row['ext'] = row['ext'].lower()
        data.append(row)
    f.close()

    with open('../data/reference.yaml', 'w') as f:
        yaml.dump(data, f)

if __name__ == "__main__":
  app()