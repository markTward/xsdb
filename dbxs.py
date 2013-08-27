'''
Created on Aug 26, 2013

@author: mward
'''

db = {}
idx = {}
bi = {}

TRANLEVEL=0
    
def begin():
    global TRANLEVEL
    TRANLEVEL += 1
    if TRANLEVEL not in bi:
        bi[TRANLEVEL] = []
    print 'BEGIN TRANLEVEL==>', TRANLEVEL, bi
    
def rollback():
    global TRANLEVEL    
    if TRANLEVEL >= 1:
        print 'BACKOUT queue', bi[TRANLEVEL]
        for trans in bi[TRANLEVEL]:
            print '  BACKOUT transaction', trans
            if trans[1] != None:
                dbset(trans[0],trans[1],rollback=True)
            else:
                dbunset(trans[0], rollback=True)
        print 'ROLLBACK TRANLEVEL==>', TRANLEVEL, bi
        bi.pop(TRANLEVEL)
        TRANLEVEL -= 1
    else:
        print 'NO TRANSACTION'

def commit():
    global bi, TRANLEVEL
    bi = {}
    TRANLEVEL = 0
    print 'COMMIT bi=>', bi
    
def biwrite(k,v):
    global TRANLEVEL
    global bi
    
    if TRANLEVEL < 1: return

    if k in db:
        bi[TRANLEVEL].insert(0, (k,db[k]))
    else:
        bi[TRANLEVEL].insert(0, (k,None))   

    print 'bi queue==>',bi
    
def dbset(k,v,rollback=False):
    # if transaction
    if not rollback:
        biwrite(k,v)
    
    # write to database
    db[k] = v
    
    # create index
    if v in idx:
        idx[v] = idx[v].union(k)
    else:
        idx[v] = set()
        idx[v].update(k)
    
    
def dbget(k):
    return db[k] if k in db else 'NULL'

def dbunset(k,rollback=False):
    if k in db:
        # if not rollback, write to bi
        if not rollback:
            biwrite(k,db[k])
        # clear from index
        vtemp = db[k]
        idx[vtemp].remove(k)
        if len(idx[vtemp]) == 0:
            idx.pop(vtemp)
        # remove from database    
        db.pop(k)

def numeqto(v):
    return len(idx[v]) if v in idx else 'NULL'

def show_dbbd():
    print 'db{}==>', db
    #print 'idx{}==>', idx
    
dbset('a',10)
dbset('b', 20)
begin()
dbset('b',-99)
dbset('a', -101)
dbset('c',20)
begin()
dbset('d',100)
dbunset('c')
print 'before ROLLBACK', show_dbbd()
rollback()
print 'after ROLLBACK',show_dbbd()
commit()
rollback()
print 'after ROLLBACK',show_dbbd()
rollback()





        