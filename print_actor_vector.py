import sys

from test import *

args = sys.argv
if len(args) < 3:
    print 'Requires two args: actorid and model'
    exit()

actorid = args[1]
model = args[2]
if model not in ['TF','TF-IDF']:
    print "Wrong Model parameter"
    exit()
vector = compute_TASK1(actorid,model)
vector.sort(key=lambda student: student[1], reverse=True)
print vector

#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"