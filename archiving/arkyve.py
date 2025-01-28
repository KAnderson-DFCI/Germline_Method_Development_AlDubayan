import sys
from time import sleep
import posixpath
from collections import defaultdict as ddict
from collections.abc import Sequence, Mapping
from os import makedirs
from os.path import join

from multiprocessing import Pool
import json
import re

import pandas as pd
from google.cloud.storage import Client
from firecloud.fiss import fapi as fcl

import kterra as kt

LL = 0
def overline(*args):
  global LL
  args = [str(a) for a in args]
  message = ' '.join(args)
  cL = len(message)
  if cL < LL:
    message += ' ' * (LL-cL)
  print('\r' + message, end='')
  LL = cL

class WorkspaceMigrator():

  def __init__(self, gproj: str, wsn: str, akn: str):
    self.gclient = Client(gproj)

    self.ws = kt.Workspace(wsn, gproj)
    self.wsbucket = self.gclient.bucket(self.ws.bucket)
    self.wspref = posixpath.join('gs://', self.wsbucket.name) + '/'
    self.akbucket = self.gclient.bucket(akn)
    self.akpref = posixpath.join('gs://', self.akbucket.name) + '/'

    self.connection_info = (self.gclient.project,
                            self.wsbucket.name,
                            self.akbucket.name)

    self.file_map = json_load(join('migration', wsn, 'file_map.json'))
    self.attr_updates = {}
    self.entity_updates = ddict(lambda: ddict(dict))

  # Planning
  def plan_migration(self):
    overline("Planning file transfer...")

    self.plan_table(self.ws.attr_table, 'attributes')

    for k in self.ws.list_tables():
      df = self.ws.get_table(k)
      self.plan_table(df, k)

    self.archive_json('file_map.json', self.file_map)
    json_dump(self.file_map, join('migration', self.ws.name, 'file_map.json'))
    
  def plan_table(self, df: pd.DataFrame, table_name: str):
    ET = df.index.name == 'id'
    for ent, row in df.iterrows():
      for col, val in row.items():
        new_val = self.plan_value(table_name, str(ent) if ET else '', col, val)
        if new_val:
          if ET: self.entity_updates[table_name][ent][col] = new_val
          else: self.attr_updates[col] = new_val

  def plan_value(self, table: str, ent: str, col: str, val, i:int|None = None):
    '''
      This method considers a cell in the data table.
      It returns None if the cell will not need to be updated post migration.
      It returns the appropriate new contents of the cell if an update is needed.
    '''
    if isinstance(val, str):
      return self.plan_string(table, ent, col, val, i)
    
    if isinstance(val, list):
      if len(val) == 0 or not isinstance(val[0], str):
        return None
      
      changes = []
      for i, v in enumerate(val):
        dest = self.plan_string(table, ent, col, v, i)
        if dest: changes.append([i, dest])
      if len(changes) == 0: return None

      aggregate = list(val)
      for i, c in changes:
        aggregate[i] = c
      return aggregate
    
    if isinstance(val, kt.JSONEntry):
      self.json_context = (table, ent, col)
      need_update, new_val, _ = self.plan_json(val.value)
      if need_update: return kt.JSONEntry(new_val)
      
    return None
  
  def plan_json(self, A, i=0):
    U = False
    B = A
    if isinstance(A, str):
      if A in self.file_map:
        U = True
        B = self.file_map[A]
      else:
        b = self.plan_string(*self.json_context, A, i)
        if b is not None:
          U = True
          B = b
          i += 1
    elif isinstance(A, Sequence):
      B = list()
      for a in A:
        nU, b, i = self.plan_json(a, i)
        U |= nU
        B.append(b)
    elif isinstance(A, Mapping):
      B = dict()
      for k, a in A.items():
        nU, q, i = self.plan_json(k, i)
        U |= nU
        nU, b, i = self.plan_json(a, i)
        U |= nU
        B[q] = b
    return (U, B, i)
        
  
  def plan_string(self, table: str, ent: str, col: str, val: str, i:int|None = None):
    if not val.startswith('gs://'): return None
    if not self.is_workspacefile(val): return None
    if val in self.file_map: return self.file_map[val]
    dest = self.plan_destination(table, ent, col, val, i)
    self.file_map[val] = dest
    return dest

  def is_workspacefile(self, val: str):
    return val.startswith(self.wspref)
  
  def plan_destination(self, table: str, ent: str, col: str, val: str, i:int|None = None):
    base = posixpath.basename(val)
    dest = posixpath.join(self.akpref,
                     self.ws.name,
                     table, ent, col,
                     str(i) if i is not None else '',
                     base)
    return dest
  
  # Refiling
  def migrate_files(self, n=4):
    counts = {k:0 for k in 'MEDC'}
    problems = ddict(list)

    if not self.file_map:
      overline('No files planned for migration')
      sleep(2)
      return counts, problems
    
    stat_string = f'Migrating {len(self.file_map)} files:' + '{C}(C) = {D}(D) + {M}(M) + {E}(E)'
    with Pool(processes=n,
              initializer=setup_connections,
              initargs=self.connection_info) as p:
      
      iou = p.imap_unordered(migrate_file_v, self.file_map.items())
      for srce, status in iou:
        counts['C'] += 1
        counts[status] += 1
        if status in 'ME':
          problems[status].append((srce, self.file_map[srce]))
        overline(stat_string.format(**counts))
    return counts, problems

  def update_tables(self, n=4):
    overline('Updating tables...     ')

    if len(self.attr_updates) == 0:
      overline('No workspace attributes to update')
      sleep(2)
    else:
      attr_updicts = []
      for attr, val in self.attr_updates.items():
        if isinstance(val, list):
          val = kt.attlist(val)
        attr_updicts.append(fcl._attr_set(attr, val))
      did = stubbornly(self.update_attrs, attr_updicts, _tries=10)
      if not did:
        raise ValueError('Failed to update workspace attributes.')

    counts = {}
    problems = {}
    if len(self.entity_updates) == 0:
      overline('No entities to update')
      sleep(2)
    else:
      with Pool(processes=n,
                initializer=setup_workspace,
                initargs=(self.ws.name,)) as p:
        for table, entdata in self.entity_updates.items():
          tcounts = {k:0 for k in 'SFC'}
          tprobs = []

          tt = len(entdata)
          stat_string = f'Updating {tt} {table} entities:' + '{C}(C) = {S}(S) + {F}(F)'
          
          iou = p.imap_unordered(force_update_entity_v,
                                map(lambda x: (table, *x), entdata.items()))
          
          for entity, status in iou:
            tcounts['C'] += 1
            tcounts['S' if status else 'F'] += 1
            if not status:
              tprobs.append(entity)
            overline(stat_string.format(**tcounts))
          
          counts[table] = tcounts
          problems[table] = tprobs
    
    return counts, problems

  def update_attrs(self, updicts):
    res = fcl.update_workspace_attributes(self.ws.project, self.ws.name, updicts)
    return kt.check_request(res)

  def cleanup_old(self):
    overline('Deleting old files...')

    for srce, dest in self.file_map.items():
      srce_name = srce.removeprefix(self.wspref)
      srce_blob = self.wsbucket.blob(srce_name)

      dest_name = dest.removeprefix(self.akpref)
      dest_blob = self.akbucket.blob(dest_name)

      if dest_blob.exists() and srce_blob.exists():
        srce_blob.delete()

  def archive_meta(self):
    overline('Archiving Models and Metadata')
    desc = self.ws.attr_table['description'][0]
    meta = dict(self.ws.metadata['workspace'])
    meta['_desc_'] = desc

    self.archive_json('workspace_meta.json', meta)

    url = f"workspaces/{self.ws.project}/{self.ws.name}/exportAttributesTSV"
    res = fcl_get(url)

    self.archive_attachment(res, '_tsv')

    for k in self.ws.list_tables():
      res = fcl.get_entities_tsv(self.ws.project, self.ws.name, k, model='flexible')
      if kt.check_request(res): raise ValueError()
      self.archive_attachment(res, '_tsv')

  def archive_attachment(self, res, pref=''):
    info = res.headers['Content-Disposition']
    m = re.search(r'filename="(.+)"', info)
    fn = m[1]

    dest_blob = self.akbucket.blob(posixpath.join(self.ws.name, pref, fn))
    if dest_blob.exists():
      dest_blob.delete()

    dest_blob.upload_from_string(res.content,
      content_type=res.headers['content-type'])
    
  def archive_json(self, name, obj, pref=''):
    json_blob = self.akbucket.blob(posixpath.join(self.ws.name, pref, name))
    if json_blob.exists(): json_blob.delete()
    json_blob.upload_from_string(json.dumps(obj), content_type='application/json')

def fcl_get(url):
  res = fcl.__get(url)
  if kt.check_request(res): raise ValueError()
  return res


### file migration context
gclient = None
srce_bucket = None
srce_pref = None
dest_bucket = None
dest_pref = None

def setup_connections(gproject, sbn, dbn):
  global gclient, srce_bucket, srce_pref, dest_bucket, dest_pref
  gclient = Client(gproject)
  srce_bucket = gclient.bucket(sbn)
  srce_pref = posixpath.join("gs://", sbn) + '/'
  dest_bucket = gclient.bucket(dbn)
  dest_pref = posixpath.join("gs://", dbn) + '/'


def migrate_file_v(args):
  try:
    srce, dest = args
    return migrate_file(srce, dest)
  except:
    return (srce, 'E')

def migrate_file(srce, dest):
  srce_name = srce.removeprefix(srce_pref)
  srce_blob = srce_bucket.blob(srce_name)
  
  dest_name = dest.removeprefix(dest_pref)
  dest_blob = dest_bucket.blob(dest_name)

  if not srce_blob.exists():
    return srce, 'M'
  if dest_blob.exists():
    if dest_blob.size == srce_blob.size:
      return srce, 'D'
    dest_blob.delete()
  
  token, _, _ = dest_blob.rewrite(srce_blob)
  while token != None:
    token, _, _ = dest_blob.rewrite(srce_blob, token=token)

  return srce, 'D'

def upload_file(fn, dest):
  dest_name = dest.removeprefix(dest_pref)
  dest_blob = dest_bucket.blob(dest_name)

  if dest_blob.exists():
    dest_blob.delete()

  dest_blob.upload_from_filename(fn)


### workspace context
tworkspace: kt.Workspace = None
def setup_workspace(wsn):
  global tworkspace
  tworkspace = kt.Workspace(wsn)


def force_update_entity_v(args):
  return force_update_entity(*args)

def force_update_entity(table, entity, attr_val):
  return entity, stubbornly(tworkspace.update_entity, table, entity, **attr_val, _tries=10)


def stubbornly(indfunc, *args, _tries=3, **kwargs):
  for i in range(_tries):
    if not indfunc(*args, **kwargs):
      return True
  return False

### Util
def json_dump(what, where):
  with open(where, 'w') as out:
    json.dump(what, out, default=json_kt_types)

def json_load(where, default={}):
  try:
    with open(where, 'r') as inp:
      return json.load(inp)
  except:
    return default

def json_kt_types(obj):
  if isinstance(obj, kt.ReferenceList):
    return {'references': obj.entity_type, 
            'items': obj.entity_list}
  elif isinstance(obj, kt.JSONEntry):
    return {'json': obj.value}
  raise TypeError()

### Pipelines
def migrate_workspace(gproj, wsn, akn, n=4):
  where = join("migration", wsn)
  makedirs(where, exist_ok=True)

  migrator = WorkspaceMigrator(gproj, wsn, akn)
  migrator.plan_migration()

  json_dump(migrator.entity_updates, join(where, 'entity_plan.json'))
  json_dump(migrator.attr_updates, join(where, 'attr_plan.json'))

  counts, problems = migrator.migrate_files(n)
  if counts['C'] != counts['D']:
    print('\nFailed to migrate all files, sending problems to "migration_problems.json"')
    json_dump(problems, join(where, 'migration_problems.json'))
    return False

  counts, problems = migrator.update_tables(n)
  concord = [c['C'] == c['S'] for c in counts.values()]
  if (len(counts) != 0) and not all(concord):
    print('\nFailed to update all entities, sending problems to "update_problems.json"')
    json_dump(problems, join('where', 'update_problems.json'))
    return False
  
  migrator.cleanup_old()
  migrator.archive_meta()
  return True

# TODO make workspace reconstructor and verify all information is present
if __name__ == "__main__":
  if len(sys.argv) < 4:
    print('Bad Job')
    sys.exit()
  
  gproj, wsn, akn = sys.argv[1:4]
  n = 4
  if len(sys.argv) > 4:
    n = int(sys.argv[4])
  
  migrate_workspace(gproj, wsn, akn, n)