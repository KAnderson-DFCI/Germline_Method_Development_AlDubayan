import json
import posixpath
import re
import sys
from collections import defaultdict as ddict
from multiprocessing import Pool
from os import makedirs
from os.path import join
from time import sleep

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
      self.bkn,
      self.akn,
    )

    self.file_map = json_load(join("migration", wsn, "misc_file_map.json"))

  def plan_files(self, excludes=[]):
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
    json_dump(self.file_map, join("migration", self.ws.name, "misc_file_map.json"))
  
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

  def cleanup_old(self):
    overline("Deleting old files...")

    for srce, dest in self.file_map.items():
      srce_name = srce.removeprefix(self.wspref)
      srce_blob = self.wsbucket.blob(srce_name)

      dest_name = dest.removeprefix(self.akpref)
      dest_blob = self.akbucket.blob(dest_name)

      if dest_blob.exists() and srce_blob.exists():
        srce_blob.delete()


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


# Pipeline
def migrate_bucket_files(wsn, akn, n=4):
  where = join("migration", wsn)
  makedirs(where, exist_ok=True)

  migrator = BucketMigrator(wsn, akn)
  migrator.plan_files()

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

  migrator.cleanup_old()
  return True


if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("Requires at least two arguments; source workspace name and archive bucket name.\nA number of cpus to use may be included as a third argument, default 4.")
    sys.exit()

  wsn, akn = sys.argv[1:3]
  n = 4
  if len(sys.argv) > 3:
    n = int(sys.argv[3])

  migrate_bucket_files(wsn, akn, n)