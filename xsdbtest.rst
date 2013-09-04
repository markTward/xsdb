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

===========
from rstdoc
===========
Doctest blocks are interactive 
Python sessions. They begin with 
"``>>>``" and end with a blank line.

>>> print "This is a doctest block." 
This is a doctest block.
