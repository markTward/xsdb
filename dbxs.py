'''
Created on Aug 26, 2013

@author: mward
'''

import sys

db = {}
idx = {}
bi = {}

TRANLEVEL=0
DEBUG=1
    
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
            if trans[1] != None:
                dbset(trans[0],trans[1], is_rollback=True)
            else:
                dbunset(trans[0], is_rollback=True)
        bi.pop(TRANLEVEL)
        TRANLEVEL -= 1
    else:
        print 'NO TRANSACTION'

def commit():
    global bi, TRANLEVEL
    if DEBUG > 0: print 'BEFORE COMMIT bi=>', bi
    bi = {}
    TRANLEVEL = 0
    if DEBUG > 0: print 'AFTER COMMIT bi=>', bi
    
def biwrite(k,v):
    global TRANLEVEL
    global bi
    
    if TRANLEVEL < 1: return

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
    # if transaction
    if not is_rollback:
        biwrite(k,v)
        
    # if update and not create, remove previous index
    if k in db:
        idx_remove(db[k],k)

    # write to database
    db[k] = v
    
    # create new or update existing index
    if v in idx:
        idx[v] = idx[v].union((k,))
    else:
        idx[v] = set()
        idx[v].update((k,))
    
    
def dbget(k):
    return db[k] if k in db else 'NULL'

def dbunset(k,is_rollback=False):
    if k in db:
        # if not rollback, write to bi
        if not is_rollback:
            biwrite(k,db[k])
        # clear from index
        #vtemp = db[k]
        
        idx_remove(db[k],k)
            
        # remove from database    
        db.pop(k)

def numeqto(k):
    return len(idx[k]) if k in idx else 0

def show_dbbd():
#     print 'db{}==>', db
#     print 'idx{}==>', idx
    print db, idx
   
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
    
    if DEBUG > 1: print 'input cmd_input::split==>', cmd, icmd, params
    
    cmdlist = {'set':dbset, 'unset':dbunset, 'get':dbget, 'numeqto':numeqto, 
               'begin':begin,'rollback':rollback,'commit':commit, 
               'show':show_dbbd, 'end':exit}
    
    try:
        result = cmdlist[icmd](**params)
        if result != None: print result
    except:
        print 'cmd_exec failed for:', icmd, params
    
cmd_input = ''
if __name__ == '__main__':   
    while cmd_input.lower() != 'end': 
        try:
            cmd_input = sys.stdin.readline().strip()
        except KeyboardInterrupt:
            break
        
        if not cmd_input:
            break
          
        cmd_exec(cmd_input.lower().split())    