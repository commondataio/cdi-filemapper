import json
import csv
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


import typer


app = typer.Typer()

SAMPLE_FILE = '../data/unrefined/sample.tsv'
RULES_FILE = '../data/reference/rules.yaml'



@app.command()
def identify(text):
    """Identifies file type by text profilded"""
    rules  = {}
    f = open(RULES_FILE, 'r', encoding='utf8')
    rules_data = yaml.load(f, Loader=Loader)
    f.close()
    for row in rules_data:
        rules[row['text']] = row

    if text in rules.keys():
       print('Input text: %s, detected mime %s, %s extension' % (text, rules[text]['mime'], rules[text]['ext']))
    else:
       print('Filetype not found')


@app.command()
def test():
    """Test sample file"""
    rules  = {}
    f = open(RULES_FILE, 'r', encoding='utf8')
    rules_data = yaml.load(f, Loader=Loader)
    f.close()
    for row in rules_data:
        rules[row['text']] = row

    f = open(SAMPLE_FILE, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter='\t')
    n = 0
    for row in reader:
        n += 1
        detected = False
        if len(row['mime']) > 0:
           text = row['mime'].lower()
           if text in rules.keys():                
#               print('%d: By mime, input text: %s, detected mime %s, %s extension' % (n, text, rules[text]['mime'], rules[text]['ext']))
               detected = True
        if not detected and len(row['format']) > 0:
           text = row['format'].lower()
           if text in rules.keys():
#               print('%d: By format, input text: %s, detected mime %s, %s extension' % (n, text, rules[text]['mime'], rules[text]['ext']))
               detected  = True
        ext = row['url'].rsplit('.', 1)[-1]
        if not detected and len(ext) < 8 and ext.isalnum():
           text = ext.lower()
           if text in rules.keys():
#               print('%d: By urlext, input text: %s, detected mime %s, %s extension' % (n, text, rules[text]['mime'], rules[text]['ext']))
               detected  = True
        if not detected:
           print('%d: Filetype not found for mime %s, format %s and url %s' % (n, row['mime'], row['format'], row['url'].encode('utf8')))


if __name__ == "__main__":
    app()