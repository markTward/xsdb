'''
dbxs.py
Created on Aug 26, 2013
@author: mward

challenge to create an xtra small DB with set,get,unset and transaction scoping (global commit)

'''

from __future__ import print_function
import sys, shlex


db = {}
idx = {}
bi = {}

TRANLEVEL=0
DEBUG=False

def debug():
    global DEBUG
    DEBUG = not DEBUG

def debugprint(*args):
    if DEBUG:
        print (args)
    
def begin():
    global TRANLEVEL
    TRANLEVEL += 1
    if TRANLEVEL not in bi:
        bi[TRANLEVEL] = {}
    debugprint('BEGIN TRANLEVEL==>', TRANLEVEL, bi)
    
def rollback():
    global TRANLEVEL    
    if TRANLEVEL >= 1:
        debugprint('ROLLBACK TRANLEVEL==>', TRANLEVEL, bi[TRANLEVEL])
        for db_key ,rollback_value in bi[TRANLEVEL].items():
            debugprint('  BACKOUT transaction to', db_key ,rollback_value)
            # rollback by executing previous state's set and unset commands
            # is_rollback=True blocks rewrite to bi file blocked
            if rollback_value != None:
                dbset(db_key, rollback_value, is_rollback=True)
            else:
                dbunset(db_key, is_rollback=True)
        # remove just restore bi level and decrement to previous transaction rollback level        
        bi.pop(TRANLEVEL)
        TRANLEVEL -= 1
    else:
        return 'NO TRANSACTION'

def commit():
    global TRANLEVEL
    if TRANLEVEL > 0:
        bi.clear()
        TRANLEVEL = 0
    else:
        return 'NO TRANSACTION'
    
def biwrite(k,v,is_rollback=False):
    # only write to bi-image at least one 'begin' has been invoked not a 'rollback'
    if TRANLEVEL < 1 or is_rollback: return

    # maintain state for k,v pair or k,None if it is new
    if not k in bi[TRANLEVEL]:
        if k in db:
            bi[TRANLEVEL][k]=db[k]
        else:
            bi[TRANLEVEL][k]=None 

    debugprint('bi queue==>',bi)
    
def idx_remove(k,v):
    if not k in idx: return
    idx[k].remove(v)
    if len(idx[k]) == 0:
        idx.pop(k)

    
def dbset(k,v,is_rollback=False):
    # send transaction biwriter
    # TODO: can biwrite be made into a decorator?
    biwrite(k,v,is_rollback)
        
    # if update and not create, remove previous index entry corresponding to previous value
    if k in db:
        idx_remove(db[k],k)

    # create / update
    db[k] = v
    
    # create new or update existing index based on current value        
    if v not in idx:
        idx[v] = set()
        
    idx[v] = idx[v].union((k,))
   

def dbget(k):
    return db[k] if k in db else 'NULL'


def dbunset(k,is_rollback=False):
    if k in db:
        # send to bi writer; TODO: make into decorator?
        biwrite(k,db[k],is_rollback)
        # remove index for value of k
        idx_remove(db[k],k)
        # remove entry from database    
        db.pop(k)

def numequalto(k):
    return len(idx[k]) if k in idx else 0

def show():
    return 'db{}==>',db, 'idx{}==>', idx, 'bi{}==>',bi

def reset():
    global db, idx, bi, TRANLEVEL
    # reset all database, index, before-image and transaction level collection-globals
    db, idx, bi, TRANLEVEL = {},{},{},0
    
def help():
    print ('SET <key> <value> :: set a key/value pair')
    print ('UNSET <key> :: remove a key/value pair')
    print ('GET <key> :: retrieve value for given key')
    print ('NUMEQUALTO <value> :: total number of keys set to value')
    print ('BEGIN :: start a transaction level')
    print ('ROLLBACK :: backout all transactions for current block')
    print ('COMMIT :: commit all transactions across all block')
    print ('SHOW :: dump all database, index and transaction level values')
    print ('RESET :: delete all key/value pairs and clear all transaction blocks')
    print ('HELP :: show these commands')
    print ('DEBUG :: toggle debugging statements on/off')
    
def cmd_exec(cmd):
    # assign functions to all available commands
    cmdlist = {'set':dbset, 'unset':dbunset, 'get':dbget, 'numequalto':numequalto, 
               'begin':begin,'rollback':rollback,'commit':commit, 
               'show':show, 'reset':reset, 'debug':debug, 'help':help}
    # commands producing output
    printresult = ['get','numequalto','show', 'commit', 'rollback']
    
    try:
        assert cmd[0] in cmdlist    
    except:
        print ("cmd_exec() ERROR: unknown command",cmd[0])
        return
    
    # execute function defined in cmdlist referenced by cmd[0]
    # provide remaining command line elements as positional parameters
    try:
        result = cmdlist[cmd[0]](*cmd[1:])
        if cmd[0] in printresult and result != None: print (result)
    except:
        print ('cmd_exec failed for:', cmd[0], cmd[1:])
      
if __name__ == '__main__':
    while True: 
        try:
            cmd_input = sys.stdin.readline().strip()
        except KeyboardInterrupt:
            break
        
        # if END command or freestanding <RETURN>, exit
        if not cmd_input or cmd_input.lower() == 'end':
            break
          
        # shlex enables simple multi-element key/value string handling for more interesting db
        cmd_exec(shlex.split(cmd_input.lower()))    
