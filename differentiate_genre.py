import sys

from core import *

args = sys.argv
if len(args) < 4:
    print 'Requires three args: genre1, genre2 and model'
    exit()

genre1 = args[1]
genre2 = args[2]
model = args[3]
if model not in ['TF-IDF-DIFF','P-DIFF1','P-DIFF2']:
    print "Wrong Model parameter. Valid options TF-IDF-DIFF / P-DIFF1 / P-DIFF2"
    exit()
compute_TASK4(genre1,genre2,model)

#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"