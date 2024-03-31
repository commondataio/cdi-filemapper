import sys
import json
import csv
import os
import yaml
import typer

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

app = typer.Typer()

FILENAMES = ['dateno-file-formats.tsv', 'dateno-mimetypes.tsv']


def load_json(filename, key):
    f = open(filename, 'r', encoding='utf8')
    data = json.load(f)
    f.close()
    out = {}
    for d in data:
        out[d[key]] = d
    return out

def load_csv(filename, key, delimiter='\t'):
    f = open(filename, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter=delimiter)
    data = list(reader)
    f.close()
    out = {}
    for d in data:
        out[d[key]] = d
    return out

def load_properties(filename):
    data = {}
    f = open(filename, 'r', encoding='utf8')    
    for l in f:
        l = l.strip()
        if len(l) == 0: continue
        if l[0] == '#': continue
        if l.find('=') == -1: continue
        k, v = l.split('=', 1)
        data[k] = v
    return data


def load_yaml(filename):
    f = yaml.load(open(filename, 'r', encoding='utf8'), Loader=Loader)
    return f



class Builder:
    def __init__(self):
        pass

    def build_unrefined(self):
        self.workset = load_csv('../data/unrefined/reference_mimes.tsv', key='mime')
        self.names = load_properties('../data/unrefined/MimeTypeDisplay.properties')
        mimetypes = []
        extensions = {}
        extlist = []        
        rules = []
        for mime, item in self.workset.items():
            if mime in self.names.keys():
                item['name'] = self.names[mime]
                del item['count']
            if len(item['exts']) > 0:
                exts = item['exts'].split(',')
                for ext in exts: 
                     if ext not in extensions.keys():
                         extensions[ext] = ({'ext' : ext, 'name' : item['name'], 'default_mime' : mime if len(item['default_mime']) == 0 else item['default_mime'],  'mimetypes' : [mime], 'categories': item['categories']})
                     else:
                          if mime not in extensions[ext]['mimetypes']:
                              extensions[ext]['mimetypes'].append(mime)
            item['exts'] = item['exts'].split(',')
            mimetypes.append(item) 


        with open('../data/reference/mimes.yaml', 'w') as f:
            yaml.dump(mimetypes, f)

        with open('../data/reference/exts.yaml', 'w') as f:
            yaml.dump(list(extensions.values()), f)


    def build_rules(self):     
        self.rules = load_csv('../data/unrefined/rules.tsv', key='text') 
        rules = []
        exts = load_yaml('../data/reference/exts.yaml')        
        mimes = load_yaml('../data/reference/mimes.yaml')
        extensions = {}
        for ext in exts:
            extensions[ext['ext']] = ext
        for rule in self.rules.values():
            if rule['ext'] not in extensions.keys(): 
                print(rule['text'], rule['ext'])
                continue
            ext = extensions[rule['ext']]
            rule['mime'] = ext['default_mime']
            del rule['count']
            rule['text'] = rule['text'].lower()
            rule['categories'] = ext['categories'].split(',') if len(ext['categories']) > 0 else ['other']
            rules.append(rule)
        for ext in exts:
            rule = {'ext': ext['ext'], 'mime' : ext['default_mime'], 'text' : ext['ext'].lower()}
            rules.append(rule)
            rule = {'ext': ext['ext'], 'mime' : ext['default_mime'], 'text' : ext['ext'].strip('.').lower(), 'categories' : ext['categories'].split(',')  if len(ext['categories']) > 0 else ['other']}
            rules.append(rule)
        for m in mimes:
            rule = {'ext': m['default_ext'], 'mime' : m['mime'], 'text' : m['mime'].lower(), 'categories' : m['categories'].split(',') if len(m['categories']) > 0 else ['other']}
            rules.append(rule)

        with open('../data/reference/rules.yaml', 'w') as f:
            yaml.dump(rules, f)


@app.command()
def run():
    builder = Builder()
    builder.build_rules()
    pass

if __name__ == "__main__":
  app()