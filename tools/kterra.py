###
# This module should be entirely functional from within terra's notebook environment
# Instructions are provided below for supporting local operation.
#
# **Windows Instructions**
# 1. Install gcloud sdk
# 2. open gcloud commnd line<sup>*</sup> to initialize defaults and account info
# 3. run `gcloud auth application-default login` to set up application default credentials ("ADC")
# 4. open python environment and 
#    i. `pip install google-cloud-storage`
#   ii. `pip install firecloud`

import os, sys
from posixpath import join, relpath
from collections import defaultdict as ddict
from collections.abc import Mapping

import pandas as pd
import numpy as np
import json

from google.cloud.storage import Client
from firecloud.fiss import fapi as fcl


### google storage clients
class ClientCache(dict):
    def __missing__(self, key):
        return Client(key)


### firecloud workspaces
def getWorkspaces(refresh=False, fields=None, fmap=None):
    global last_request, fc_workspaces
    if not refresh and fc_workspaces: return fc_workspaces

    preserve = False
    if not fields and not fmap:
        fields = [
            'accessLevel',
            'workspace.name',
            'workspace.namespace',
            'workspace.workspaceId',
            'workspace.bucketName',
            'workspace.createdBy',
            'workspace.billingAccount',
        ]
        fmap = key_ep
        preserve = True

    last_request = fcl.list_workspaces(",".join(fields))
    if last_request.status_code != 200:
        print('Bad Request', last_request.status_code, last_request.reason, file=sys.stderr)
        return None
    
    table = tabulate(last_request.json(), fields, fmap)
    table.set_index('name', inplace=True)
    if preserve:
        fc_workspaces = table
    return table


def getWorkspaceBucket(name):
    return fc_workspaces.loc[name].bucketName

def getBucketWorkspace(bucket):
    bucket = bucket.lstrip('gs://')
    return fc_workspaces[fc_workspaces.bucketName == bucket].index[0]


class Workspace:

    def __init__(self, name, gproject='same'):
        self.name = name
        try:
            wsdata = fc_workspaces.loc[name]
        except KeyError as e:
            print(f"Workspace {name} not found!", file=sys.stderr)
            raise KeyError(f"Workspace {name} not found!") from e

        self.project = wsdata.namespace
        self.bucket = wsdata.bucketName
        if gproject:
            if gproject == 'same': gproject = self.project
            self.gclient = clients[gproject]
            self.gbucket = self.gclient.get_bucket(self.bucket)
        else:
            self.glient = None
            self.gbucket = None

    def setGProject(self, gproject):
        self.gclient = clients[gproject]
        self.gbucket = self.gclient.get_bucket(self.bucket)

    def check_request(self):
        if self.last_request.status_code != 200:
            print('Bad Request:',
                  self.last_request.status_code,
                  self.last_request.reason)
            return True
        return False

    def getDataModel(self, tab):
        self.last_request = fcl.get_entities(self.project, self.name, tab)
        if self.check_request(): return None
        entities = self.last_request.json()
        
        transf = ddict(list)
        
        keys = set()
        for e in entities:
            keys |= e['attributes'].keys()
        
        for e in entities:
            transf['name'].append(e['name'])
            for k in keys:
                if k not in e['attributes']:
                    transf[k].append(None)
                    continue
                v = e['attributes'][k]
                if type(v) == dict: # arrays are nested
                    v = v['items'] # so v is a list
                    if type(v[0]) == dict: # entity references are nested _again_
                        v = [d['entityName'] for d in v]
                transf[k].append(v)

        dm = pd.DataFrame(transf)
        dm.set_index('name', inplace=True)
        return dm
    
    def updateEntity(self, tab, ent, **attr_val):
        req = []
        for attr, val in attr_val.items():
            if type(val) == list:
                val = attlist(val)
        
            req.append({
                "op": "AddUpdateAttribute",
                "attributeName": attr,
                "addUpdateAttribute": val
            })
        self.last_request = fcl.update_entity(self.project, self.name, tab, ent, req)
        self.check_request()

    def listBucketContents(self, prefix='', delimiter='/', glob=''):
        if not self.gclient:
            print("No google-project specified to access underlying bucket.",
                  file=sys.stderr)
            return
        
        blobs = self.gbucket.list_blobs(prefix=prefix, delimiter=delimiter, match_glob=glob)
        files = set(relpath(b.name, prefix) for b in blobs)
        folders = set(relpath(b, prefix) for b in blobs.prefixes)
        return files, folders

    def listSubmissions(self, fields='brief', fmap=None):
        self.last_request = fcl.list_submissions(self.project, self.name)
        if self.check_request():
            return

        if fields == 'brief':
            fields = [
                'submissionId',
                'methodConfigurationName',
                'submissionDate',
                'submissionRoot',
                'submissionEntity.entityName']
            if not fmap:
                fmap = lambda x: key_ep(x).removeprefix('submission')
        
        table = tabulate(self.last_request.json(), fields)
        table.set_index('submissionId', inplace=True)
        if 'submissionDate' in table.columns:
            table['submissionDate'] = pd.to_datetime(table['submissionDate'])
            table.sort_values(by="submissionDate", ascending=False, inplace=True)
        if 'submissionRoot' in table.columns:
            data = table['submissionRoot']
            data = [relpath(d, join('gs://', self.bucket)) + '/' for d in data]
            table['submissionRoot'] = data
        if fmap:
            table.rename(columns=fmap, inplace=True)
            table.index.name = fmap(table.index.name)
        return table
    
    

        


#######################################################
### Methods for analyzing and converting firecloud json

# list of dicts to table
def tabulate(dlist, fields=None, fmap=None, tmap=None, delim='.'):
    '''
      Converts a list of nested dictionaries into a table.
        Each top-level dictionary in the list becomes a row.
        The nesting is flattened by concatenating keys with '.'.
        All keys and values are retained,
        with np.nan for missing values.

      fields: used to filter fields,
      fmap: used to change field names in post
    '''
    if fields is None:
        fields = agg_keys(dlist, delim=delim)
    
    by_field = ddict(list)
    for d in dlist:
        for f in fields:
            by_field[f].append(navkey(d, f, delim=delim))

    df = pd.DataFrame(by_field)
    if fmap: df.rename(columns=fmap, inplace=True)
    return df

def key_ep(k, delim='.'):
    return k.split(delim)[-1]

def navkey(d, k, delim='.'):
    a, _, b = k.partition(delim)
    if a not in d: return np.nan
    if not b: return d[a]
    return navkey(d[a], b, delim=delim)

def agg_keys(dlist, delim='.'):
    keys = set()
    for d in dlist:
        keys |= flatten(d, delim=delim)
    return keys
    
def flatten(d, prefix="", delim='.'):
    keys = set()
    for k in d:
        if isinstance(d[k], Mapping):
            keys |= flatten(d[k], prefix=prefix+k+delim, delim=delim)
        else:
            keys.add(prefix + k)
    return keys

# value/reference list to DataModel json
def attlist(l):
    return {'itemsType': 'AttributeValue',
            'items': l}

def reflist(l, t='sample'):
    return {'itemsType': 'EntityReference',
            'items': [{'entityType': t,
                       'entityName': i} for i in l] }


### Initialization
clients = ClientCache()
last_request = None
fc_workspaces = None
getWorkspaces()


# environment variables set in terra notebook environments
#   if these exist, hook up to the notebook for convenience
nbWorkspace = None
if 'WORKSPACE_NAMESPACE' in os.environ and \
   'WORKSPACE_NAME' in os.environ:
    nbWorkspace = Workspace(
        os.environ['WORKSPACE_NAMESPACE'],
        os.environ['WORKSPACE_NAME'],
        os.environ['WORKSPACE_BUCKET'])
    print('Discovered Environemnt Workspace:', nbWorkspace.name)
    print("Exposed in module variable 'nbWorkspace'")

