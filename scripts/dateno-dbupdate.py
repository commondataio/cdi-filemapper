import json
import csv
import yaml
from pymongo import MongoClient
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


import typer


app = typer.Typer()

SAMPLE_FILE = '../data/unrefined/sample.tsv'
RULES_FILE = '../data/reference/rules.yaml'



@app.command()
def dotry(text):
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


class Identifier:
    def __init__(self):
        rules  = {}
        f = open(RULES_FILE, 'r', encoding='utf8')
        rules_data = yaml.load(f, Loader=Loader)
        f.close()
        for row in rules_data:
            rules[row['text']] = row
        self.rules = rules


    def identify(self, texts):
        """Uses rules to identify file mime and format"""
        for text in texts:
            if text in self.rules.keys():                
                return self.rules[text]
        return None




def update_by_source(db, uid, identifier):    
    datasets = db['fulldb'].find({'source.uid' : uid}, {'id' : 1, 'dataset.formats' : 1, 'resources' : 1, 'source.id' : 1})
    n = 0
    identified = 0
    for item in datasets:
        out_res = []
        do_update = False
        resources = item['resources']
        formats = []
        datatypes = []
        for r in resources:
             n += 1
             texts = []
             if 'mimetype' in r.keys() and r['mimetype'] is not None and len(r['mimetype']) > 0: texts.append(r['mimetype'].lower())
             if 'format' in r.keys() and r['format'] is not None and len(r['format']) > 0: texts.append(r['format'].lower())
             if r['url'] is not None:
                 ext = r['url'].rsplit('.', 1)[-1]
                 if len(ext) < 8 and ext.isalnum():
                     texts.append(ext.lower())             
             if len(texts) > 0:
                 result = identifier.identify(texts)
                 if result is not None: 
                      identified += 1
                      r['d_mime'] = result['mime']
                      r['d_ext'] = result['ext']
                      if result['ext'] is not None and len(result['ext']) > 0:
                          if result['ext'] not in formats: formats.append(result['ext'])
                      if 'categories' in result.keys() and len(result['categories']) > 0: 
                          for c in result['categories']:
                              if c not in datatypes: datatypes.append(c)
                      do_update = True
             out_res.append(r)
        if do_update: 
            db.fulldb.update_one({'id': item['id']}, {'$set': {'resources': out_res, 'dataset.formats' : formats, 'dataset.datatypes' : datatypes}, }, upsert=False)
            print(item['id'])
            print(formats, datatypes) 

    print('UID %s, total %d, identified %d' % (uid, n, identified))

    
@app.command()
def update(dryrun=True):
    identifier = Identifier()
    client = MongoClient()
    db = client['cdisearch']
    uids = db['fulldb'].distinct('source.uid')
    print('Total sources %d' % (len(uids)))
    for uid in uids:
        update_by_source(db, uid, identifier)


@app.command()
def test(dryrun=True):
    """Update Dateno resources with new mimetype and file extension"""
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