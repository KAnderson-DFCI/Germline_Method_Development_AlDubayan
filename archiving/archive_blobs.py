import json
import posixpath
import re
import sys
from collections import defaultdict as ddict
from collections.abc import Mapping, Sequence
from multiprocessing import Pool
from os import makedirs
from os.path import join, exists
from time import sleep

import numpy as np
import pandas as pd
from firecloud.fiss import fapi as fcl
from google.cloud.storage import Client

LL = 0
def overline(*args):
  global LL
  args = [str(a) for a in args]
  message = " ".join(args)
  cL = len(message)
  if cL < LL:
    message += " " * (LL - cL)
  print("\r" + message, end="")
  LL = cL


# Migration Management
class BucketMigrator:

  def __init__(self, bkn: str, akn: str, wsn: str):
    self.gclient = Client()

    self.dtbucket = self.gclient.bucket(bkn)
    self.dtpref = posixpath.join("gs://", bkn) + "/"
    self.akbucket = self.gclient.bucket(akn)
    self.akpref = posixpath.join("gs://", akn) + "/"

    self.wsname = wsn

    self.connection_info = (
      bkn,
      akn,
    )

    self.file_map = json_load(join("migration", wsn, "misc_file_map.json"))

  def plan_files(self, excludes=[]):
    overline("Planning file transfer...")
    filtered = []
    s = 0
    for blob in self.dtbucket.list_blobs():
      fn = blob.name
      if any(re.search(p, fn) for p in excludes):
        filtered.append(fn)
        continue
      
      dfn = posixpath.join(self.akpref, self.wsname, 'misc_files', fn)
      self.file_map[fn] = dfn
    
    self.archive_json("misc_file_map.json", self.file_map)
    json_dump(self.file_map, join("migration", self.wsname, "misc_file_map.json"))
    json_dump(filtered, join("migration", self.wsname, "misc_file_filtered.json"))
  
  def migrate_files(self, n=4):
    counts = {k: 0 for k in "MEDC"}
    problems = ddict(list)

    if not self.file_map:
      overline("No files planned for migration")
      sleep(2)
      return counts, problems

    stat_string = (
      f"Migrating {len(self.file_map)} files:"
      + "{C}(C) = {D}(D) + {M}(M) + {E}(E)"
    )
    with Pool(
      processes=n, initializer=setup_connections, initargs=self.connection_info
    ) as p:

      iou = p.imap_unordered(migrate_file_v, self.file_map.items())
      for srce, status in iou:
        counts["C"] += 1
        counts[status] += 1
        if status in "ME":
          problems[status].append((srce, self.file_map[srce]))
        overline(stat_string.format(**counts))
    return counts, problems
  
  def archive_json(self, name, obj, pref=""):
    json_blob = self.akbucket.blob(posixpath.join(self.wsname, pref, name))
    if json_blob.exists():
      json_blob.delete()
    json_blob.upload_from_string(json.dumps(obj), content_type="application/json")

  def cleanup_old(self, n=4):
    counts = {k: 0 for k in "MEDC"}
    problems = ddict(dict)
    overline("Deleting old files...")

    stat_string = (
      f"Deleting {len(self.file_map)} files:"
      + "{C}(C) = {D}(D) + {E}(E)"
    )
    with Pool(
      processes=n, initializer=setup_connections, initargs=self.connection_info
    ) as p:

      iou = p.imap_unordered(safe_delete_file_v, self.file_map.items())
      for srce, status in iou:
        counts["C"] += 1
        counts[status] += 1
        if status in "ME":
          problems[status][srce] = self.file_map[srce]
        overline(stat_string.format(**counts))
    return counts, problems


### File migration context
gclient = None
srce_bucket = None
srce_pref = None
dest_bucket = None
dest_pref = None


def setup_connections(sbn, dbn):
  global gclient, srce_bucket, srce_pref, dest_bucket, dest_pref
  gclient = Client()
  srce_bucket = gclient.bucket(sbn)
  srce_pref = posixpath.join("gs://", sbn) + "/"
  dest_bucket = gclient.bucket(dbn)
  dest_pref = posixpath.join("gs://", dbn) + "/"


def migrate_file_v(args):
  try:
    srce, dest = args
    return migrate_file(srce, dest)
  except:
    return (srce, "E")


def migrate_file(srce, dest):
  srce_name = srce.removeprefix(srce_pref)
  srce_blob = srce_bucket.blob(srce_name)

  dest_name = dest.removeprefix(dest_pref)
  dest_blob = dest_bucket.blob(dest_name)

  if not srce_blob.exists():
    return srce, "M"
  if dest_blob.exists():
    if dest_blob.size == srce_blob.size:
      return srce, "D"
    dest_blob.delete()

  token, _, _ = dest_blob.rewrite(srce_blob)
  while token != None:
    token, _, _ = dest_blob.rewrite(srce_blob, token=token)

  return srce, "D"


def upload_file(fn, dest):
  dest_name = dest.removeprefix(dest_pref)
  dest_blob = dest_bucket.blob(dest_name)

  if dest_blob.exists():
    dest_blob.delete()

  dest_blob.upload_from_filename(fn)


def safe_delete_file_v(args):
  try:
    srce, dest = args
    return safe_delete_file(srce, dest)
  except:
    return (srce, "E")


def safe_delete_file(srce, dest):
  srce_name = srce.removeprefix(srce_pref)
  srce_blob = srce_bucket.blob(srce_name)

  dest_name = dest.removeprefix(dest_pref)
  dest_blob = dest_bucket.blob(dest_name)

  if dest_blob.exists() and srce_blob.exists():
    srce_blob.delete()
  
  return (srce, "D")

# Utilities
def json_load(where, default={}):
  try:
    with open(where, "r") as inp:
      return json.load(inp)
  except:
    return default


def json_dump(what, where):
  with open(where, "w") as out:
    json.dump(what, out)


def list_workspaces():
  fields = [
    "accessLevel",
    "workspace.name",
    "workspace.namespace",
    "workspace.workspaceId",
    "workspace.bucketName",
    "workspace.createdBy",
    "workspace.billingAccount",
    "workspace.googleProject",
  ]
  fmap = key_ep

  last_request = fcl.list_workspaces(",".join(fields))
  if check_request(last_request):
    return None

  table = tabulate(last_request.json(), fields, fmap)
  table.set_index("name", inplace=True)
  return table


def check_request(req):
  if req.status_code != 200:
    print("Bad Request:", req.status_code, req.reason, req.content)
    return True
  return False


def tabulate(dlist: list[dict], fields=None, fmap=None, delim=".", max_depth=None):
  """
  Converts a list of nested dictionaries into a table.
    Each top-level dictionary in the list becomes a row.
    The nesting is flattened by concatenating child keys recursively.
    All keys and values are retained by default,
    with np.nan for missing values in rows.

  fields: list of keys to retain,
  fmap: dict to change field names in post
  delim: key concat delimiter
  max_depth: levels to unnest
  """

  if fields is None:
    fields = agg_keys(dlist, delim=delim, max_depth=max_depth)

  by_field = ddict(list)
  for d in dlist:
    for f in fields:
      by_field[f].append(navkey(d, f, delim=delim))

  df = pd.DataFrame(by_field)
  if fmap:
    df.rename(columns=fmap, inplace=True)
  return df


def key_ep(k, delim="."):
  return k.split(delim)[-1]


def navkey(d, k, delim="."):
  a, _, b = k.partition(delim)
  if a not in d:
    return np.nan
  if not b:
    return d[a]
  return navkey(d[a], b, delim=delim)


def agg_keys(dlist, delim=".", max_depth=None):
  keys = set()
  for d in dlist:
    keys |= flatten(d, delim=delim, max_depth=max_depth)
  return keys


def flatten(d, prefix="", delim=".", _depth=0, max_depth=None):
  if (max_depth is not None) and _depth >= max_depth:
    return set(prefix + k for k in d)

  keys = set()
  for k in d:
    if isinstance(d[k], Mapping):
      keys |= flatten(d[k], prefix + k + delim, delim, _depth + 1, max_depth)
    else:
      keys.add(prefix + k)
  return keys


# Pipeline
def migrate_bucket_files(wsn, akn, n=4):
  try:
      wsdata = fc_workspaces.loc[wsn]
  except KeyError as e:
    print(f"Workspace {wsn} not found!", file=sys.stderr)
    raise KeyError(f"Workspace {wsn} not found!") from e
  
  where = join("migration", wsn)
  makedirs(where, exist_ok=True)

  migrator = BucketMigrator(wsdata.bucketName, akn, wsn)
  if len(migrator.file_map) == 0:
    migrator.plan_files([
        'PreProcessingForVariantDiscovery_GATK4',
        'FastqToCram',
      ])

  counts, problems = migrator.migrate_files(n)
  if counts["E"] != 0:
    print(
      '\nErrors occured while migrating files, sending problem files to "misc_migration_problems.json"\nPlease try again, and if problems persist, check files listed in "migration_problems.json".'
    )
    json_dump(problems, join(where, "misc_migration_problems.json"))
    return False
  if counts["M"] > 0: # this should theoretically be impossible
    print(
      '\nSome files listed in the data model could not be found.\nThe original paths will be recorded in the archive under "misc_missing_map.json", paired with the new expected archive location.'
    )
    migrator.archive_json("misc_missing_map.json", problems["M"])
    json_dump(problems["M"], join(where, "misc_missing_map.json"))

  migrator.cleanup_old(n)
  return True


def reattempt(wsn, akn, n=4):
  overline('Reattempting migration on errored files')
  try:
      wsdata = fc_workspaces.loc[wsn]
  except KeyError as e:
    print(f"Workspace {wsn} not found!", file=sys.stderr)
    raise KeyError(f"Workspace {wsn} not found!") from e
  
  where = join("migration", wsn)
  makedirs(where, exist_ok=True)

  migrator = BucketMigrator(wsdata.bucketName, akn, wsn)
  
  problems0 = json_load(join(where,'misc_migration_problems.json'), {'E':{}})
  error_file_map = dict(problems0['E'])
  
  o_map = migrator.file_map
  migrator.file_map = error_file_map
  counts, problems = migrator.migrate_files()
  if counts["E"] != 0:
    print(
      '\nErrors occured while migrating files, please check file integrity.'
    )
    return False

  migrator.file_map = o_map
  migrator.cleanup_old(n)
  return True


fc_workspaces = list_workspaces()
if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("Requires at least two arguments; source workspace name and archive bucket name.\nA number of cpus to use may be included as a third argument, default 4.")
    sys.exit()

  wsn, akn = sys.argv[1:3]
  n = 4
  if len(sys.argv) > 3:
    n = int(sys.argv[3])

  if exists(join('migration', wsn, 'misc_migration_problems.json')):
    reattempt(wsn, akn, n)
  else:
    migrate_bucket_files(wsn, akn, n)

  # python archive_blobs.py  aldubayan-lab-terra-archives