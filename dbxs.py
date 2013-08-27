#!/usr/local/bin/python
'''
dbxs.py
Created on Aug 26, 2013
@author: mward

challenge to create an xtra small DB with set,get,unset and transaction scoping (global commit)

'''

import sys, shlex

db = {}
idx = {}
bi = {}

TRANLEVEL=0
DEBUG=0
    
def begin():
    global TRANLEVEL
    TRANLEVEL += 1
    if TRANLEVEL not in bi:
        bi[TRANLEVEL] = {}
    if DEBUG > 0: print 'BEGIN TRANLEVEL==>', TRANLEVEL, bi
    
def rollback():
    global TRANLEVEL    
    if TRANLEVEL >= 1:
        if DEBUG > 0: print 'ROLLBACK TRANLEVEL==>', TRANLEVEL, bi[TRANLEVEL]
        for db_key ,rollback_value in bi[TRANLEVEL].items():
            if DEBUG > 0: print '  BACKOUT transaction to', db_key ,rollback_value
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

    if DEBUG > 0: print 'bi queue==>',bi
    
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
    
def cmd_exec(cmd):
    # assign functions to all available commands
    cmdlist = {'set':dbset, 'unset':dbunset, 'get':dbget, 'numequalto':numequalto, 
               'begin':begin,'rollback':rollback,'commit':commit, 
               'show':show, 'reset':reset}
    # commands producing output
    printresult = ['get','numequalto','show', 'commit', 'rollback']
    
    try:
        assert cmd[0] in cmdlist    
    except:
        print "cmd_exec() ERROR: unknown command",cmd[0]
        return
        
    icmd = cmd[0]
    params = {}
    
    # this only works on narrow spec, would need to push into command/argument validation
    # TODO: better param handling
    if len(cmd) == 3:
        params = {'k':cmd[1],'v':cmd[2]}
    elif len(cmd) == 2:
        params = {'k':cmd[1]}
    
    # execute function defined in cmdlist with parameter keyword blob
    try:
        result = cmdlist[icmd](**params)
        # TODO: figure out best practice for handling function return when no return defined
        if icmd in printresult and result != None: print result
    except:
        print 'cmd_exec failed for:', icmd, params
     
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
