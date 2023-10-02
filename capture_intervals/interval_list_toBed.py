import sys
from os.path import exists

ilf = sys.argv[1]
obf = sys.argv[2]

if not exists(ilf):
  print(f"Could not locate {ilf}")
  sys.exit(1)

if exists(obf):
  print(f"{obf} already exists, overwriting!")

with open(ilf, 'r') as inp,\
     open(obf, 'w') as out:
  for line in inp:
    if line[0] == '@': continue
    n, start, end, *_ = line.strip().split()
    n = 'chr' + n
    s = str(int(start)-1)
    e = str(int(end)-1)
    out.write('\t'.join([n,s,e]) + '\n')
