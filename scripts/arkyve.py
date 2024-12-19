import sys
from time import sleep
import posixpath as path
from collections import defaultdict as ddict
from collections.abc import Sequence
from os import makedirs
from os.path import join as oj

from multiprocessing import Pool
import json

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
    self.wspref = path.join('gs://', self.wsbucket.name) + '/'
    self.akbucket = self.gclient.bucket(akn)
    self.akpref = path.join('gs://', self.akbucket.name) + '/'

    self.connection_info = (self.gclient.project,
                            self.wsbucket.name,
                            self.akbucket.name)

    self.file_map = None
    self.entity_updates = None
    self.attr_updates = None

  # Planning
  def plan_migration(self):
    overline("Planning file transfer...")

    self.file_map = {}
    self.entity_updates = ddict(lambda: ddict(dict))
    self.attr_updates = {}

    self.plan_table(self.ws.attr_table, 'attributes')

    for k in self.ws.list_tables():
      df = self.ws.get_table(k)
      self.plan_table(df, k)
    
  def plan_table(self, df: pd.DataFrame, table_name: str):
    ET = df.index.name == 'id'
    for ent, row in df.iterrows():
      for col, val in row.items():
        dest = self.plan_value(table_name, str(ent) if ET else '', col, val)
        if dest:
          if ET: self.entity_updates[table_name][ent][col] = dest
          else: self.attr_updates[col] = dest

  def plan_value(self, table: str, ent: str, col: str, val, i:int|None = None):
    if isinstance(val, str):
      return self.plan_string(table, ent, col, val, i)
    if not isinstance(val, Sequence):
      return None

    sample = next(v for v in val)
    if not isinstance(sample, str):
      if isinstance(sample, Sequence):
        raise ValueError(f"Nested Arrays are not Supported; {table}, {ent}, {col}")
      return None
    
    # val is not a string, is a sequence, and a sample element is a string
    #   proceed with planning individuals and tracking changes
    changes = []
    for i, v in enumerate(val):
      dest = self.plan_string(table, ent, col, v, i)
      if dest: changes.append([i, dest])
    if len(changes) == 0: return None
    aggregate = list(val)
    for i, c in changes:
      aggregate[i] = c
    return aggregate
  
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
    base = path.basename(val)
    dest = path.join(self.akpref,
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
      sleep(5)
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
    overline('Deleting old files...     ')

    for srce, dest in self.file_map.items():
      srce_name = srce.removeprefix(self.wspref)
      srce_blob = self.wsbucket.blob(srce_name)

      dest_name = dest.removeprefix(self.akpref)
      dest_blob = self.akbucket.blob(dest_name)

      if dest_blob.exists() and srce_blob.exists():
        srce_blob.delete()

### file migration context
gclient=None
srce_bucket = None
srce_pref = None
dest_bucket = None
dest_pref = None

def setup_connections(gproject, sbn, dbn):
  global gclient, srce_bucket, srce_pref, dest_bucket, dest_pref
  gclient = Client(gproject)
  srce_bucket = gclient.bucket(sbn)
  srce_pref = path.join("gs://", sbn) + '/'
  dest_bucket = gclient.bucket(dbn)
  dest_pref = path.join("gs://", dbn) + '/'


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
  
def delete_srce(srce):
  pass


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
    out.write(json.dumps(what))

### Pipelines
def migrate_workspace(gproj, wsn, akn, n=4):
  migrator = WorkspaceMigrator(gproj, wsn, akn)

  where = wsn + "__Migration"
  makedirs(where, exist_ok=True)

  migrator.plan_migration()
  json_dump(migrator.file_map, oj(where, 'file_map.json'))
  json_dump(migrator.entity_updates, oj(where, 'entity_plan.json'))
  json_dump(migrator.attr_updates, oj(where, 'attr_plan.json'))

  counts, problems = migrator.migrate_files(n)
  if counts['C'] == 0:
    return True
  if counts['C'] != counts['D']:
    print('\nFailed to migrate all files, sending problems to "migration_problems.json"')
    json_dump(problems, oj(where, 'migration_problems.json'))
    return False

  counts, problems = migrator.update_tables(n)
  concord = [c['C'] == c['S'] for c in counts.values()]
  if not all(concord):
    print('\nFailed to update all entities, sending problems to "update_problems.json"')
    json_dump(problems, oj('where', 'update_problems.json'))
    return False
  
  migrator.cleanup_old()
  return True
  
# TODO output table tsvs to archive
# TODO delete entire 'Submissions' folder on strong success
# TODO serialize all workspace info and delete workspace
if __name__ == "__main__":
  if len(sys.argv) < 4:
    print('Bad Job')
    sys.exit()
  
  gproj, wsn, akn = sys.argv[1:4]
  n = 4
  if len(sys.argv) > 4:
    n = int(sys.argv[4])
  
  migrate_workspace(gproj, wsn, akn, n)