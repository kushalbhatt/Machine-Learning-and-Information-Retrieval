import sys

from core import *

args = sys.argv
if len(args) < 3:
    print 'Requires two args: genre and model'
    exit()

genre = args[1]
model = args[2]
if model not in ['TF','TF-IDF']:
    print "Wrong Model parameter ! valid; TF / TF-IDF "
    exit()
vector = compute_TASK2(genre,model)
vector.sort(key=lambda student: student[2], reverse=True)

for v in vector:
    print v

#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"