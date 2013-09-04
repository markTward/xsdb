===================
Test Suite for xsdb
===================
setting simple database key value pairs

>>> from xsdb import *
>>> dbset('aaa','111')
>>> db['aaa']
'111'
>>> idx['111']
set(['aaa'])
