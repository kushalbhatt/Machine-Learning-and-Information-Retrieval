import csv
import mysql.connector
from mysql.connector import errorcode
import datetime


import math

mydb = mysql.connector.connect(host='localhost',
    user='root',
    passwd='padfoot',
    db='MWD_P1')

cursor = mydb.cursor(buffered=True)
home_dir = "/home/kushalbhatt/ASU/ASU/MWD/phase1_dataset/"

create_queries = [ "create TABLE mltags (userid INT, movieid INT, tagid INT, timestamp DATETIME)" ,
            "create TABLE mlmovies (movieid INT, moviename varchar(100), genre varchar(100))",
            "create TABLE mlratings (movieid INT,userid INT, imdbid INT, rating INT,timestamp DATETIME)",
            "create TABLE mlusers (userid INT)",
            "create TABLE movie_actor (movieid INT, actorid INT, actor_movie_rank INT)",
            "create TABLE imdb_actor_info (actorid INT, name varchar(100), gender varchar(2))",
            "create TABLE genome_tags(tagid INT, tag varchar(100))",
]
'''
#create tables
for query in create_queries:
    try:
        cursor.execute(query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
'''

insert_into_queries = [
            "INSERT INTO `mltags` (`userid`,`movieid`,`tagid`,`timestamp`) VALUES(%s,%s,%s,%s)"
            "INSERT INTO `mlmovies` (`actorid`,`moviename`,`genre`) VALUES(%s,%s,%s)"
            "INSERT INTO `mlratings` (`movieid`,`userid`,`imdbid`,`rating`,`timestamp`) VALUES(%s,%s,%s,%s,%s)"
            "INSERT INTO `mlusers` (`userid`) VALUES(%s)"
            "INSERT INTO `movie_actor` (`movieid`,`actorid`,`actor_movie_rank`) VALUES(%s,%s,%s)"
            "INSERT INTO `imdb_actor_info` (`actorid`,`name`,`gender`) VALUES(%s,%s,%s)"
            "INSERT INTO `genome_tags` (`tagid`,`tag`) VALUES(%s,%s)"
]

filenames = ['mltags.csv', 'mlmovies.csv','mlratings.csv','mlusers.csv','movie-actor.csv','imdbb-actor-info.csv','genome-tags.csv']

query = "SELECT * FROM information_schema.COLUMNS  WHERE  TABLE_SCHEMA = 'MWD_P1'  AND TABLE_NAME = 'mltags'  AND COLUMN_NAME = 'weight' "
cursor.execute(query)
ret = cursor.fetchall()
if ret:
    print 'exists'
else:
    print "doesn't exist"
print ret
'''
query = "INSERT INTO `mlusers` (`userid`) VALUES(%s)"
csv_data = csv.reader(file(home_dir+'mlusers.csv'))
next(csv_data,None)

print "Copying"
for row in csv_data:
    cursor.execute(query,row)
'''
#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"