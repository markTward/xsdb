'''
dbxs.py
Created on Aug 26, 2013
@author: mward

challenge to create an xtra small DB with set,get,unset and transaction scoping (global commit)

'''

import sys

db = {}
idx = {}
bi = {}

TRANLEVEL=0
DEBUG=0
    
def begin():
    global TRANLEVEL
    TRANLEVEL += 1
    if TRANLEVEL not in bi:
        bi[TRANLEVEL] = []
    if DEBUG > 0: print 'BEGIN TRANLEVEL==>', TRANLEVEL, bi
    
def rollback():
    global TRANLEVEL    
    if TRANLEVEL >= 1:
        if DEBUG > 0: print 'REMOVE ROLLBACK TRANLEVEL==>', TRANLEVEL, bi[TRANLEVEL]
        for trans in bi[TRANLEVEL]:
            if DEBUG > 0: print '  BACKOUT transaction', trans
            # rollback by executing previous state's set and unset commands
            # is_rollback=True blocks rewrite to bi file
            if trans[1] != None:
                dbset(trans[0],trans[1], is_rollback=True)
            else:
                dbunset(trans[0], is_rollback=True)
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
    global TRANLEVEL
    
    # only write to bi-image at least one 'begin' has been invoked not a 'rollback'
    if TRANLEVEL < 1 or is_rollback: return

    # TODO:
    if k in db:
        bi[TRANLEVEL].insert(0, (k,db[k]))
    else:
        bi[TRANLEVEL].insert(0, (k,None))   

    if DEBUG > 0: print 'bi queue==>',bi
    
def idx_remove(k,v):
    if not k in idx: return
    idx[k].remove(v)
    if len(idx[k]) == 0:
        idx.pop(k)
      
def dbset(k,v,is_rollback=False):
    # send tranactionto biwriter
    # TODO: make biwrite into a decorator
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
   
def cmd_exec(cmd):

    try:
        icmd = cmd[0]        
    except:
        print "cmd_exec() ERROR in retrieving command"
        
    params = {}
    
    if len(cmd) == 3:
        params = {'k':cmd[1],'v':cmd[2]}
    elif len(cmd) == 2:
        params = {'k':cmd[1]}
    
    if DEBUG > 0: print 'input cmd_input::split==>', cmd, icmd, params
    
    cmdlist = {'set':dbset, 'unset':dbunset, 'get':dbget, 'numequalto':numequalto, 
               'begin':begin,'rollback':rollback,'commit':commit, 
               'show':show}
    
    printresult = ['get','numequalto','show', 'commit', 'rollback']
    
    try:
        result = cmdlist[icmd](**params)
        if icmd in printresult and result != None: print result
    except:
        print 'cmd_exec failed for:', icmd, params
     

if __name__ == '__main__':
    while True: 
        try:
            cmd_input = sys.stdin.readline().strip()
        except KeyboardInterrupt:
            break
        
        if not cmd_input or cmd_input.lower() == 'end':
            break
          
        cmd_exec(cmd_input.lower().split())    