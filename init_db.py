import csv
import mysql.connector
from mysql.connector import errorcode
import datetime


import math

#provide credentials and db name
mydb = mysql.connector.connect(host='localhost',
    user='root',
    passwd='padfoot',
    db='mwd')


cursor = mydb.cursor(buffered=True)

home_dir = "K:/ASU/MWD/Phase2_data/"


create_queries = [ "create TABLE mltags (userid INT, movieid INT, tagid INT, timestamp DATETIME)" ,
            "create TABLE mlmovies (movieid INT, moviename varchar(100),year INT, genre varchar(100))",
            "create TABLE mlratings (movieid INT,userid INT, imdbid INT, rating INT,timestamp DATETIME)",
            "create TABLE mlusers (userid INT)",
            "create TABLE movie_actor (movieid INT, actorid INT, actor_movie_rank INT)",
            "create TABLE imdb_actor_info (actorid INT, name varchar(100), gender varchar(2))",
            "create TABLE genome_tags(tagid INT, tag varchar(100))"]

insert_into_queries = [
            "INSERT INTO `mltags` (`userid`,`movieid`,`tagid`,`timestamp`) VALUES(%s,%s,%s,%s)",
            "INSERT INTO `mlmovies` (`movieid`,`moviename`,`year`,`genre`) VALUES(%s,%s,%s,%s)",
            "INSERT INTO `mlratings` (`movieid`,`userid`,`imdbid`,`rating`,`timestamp`) VALUES(%s,%s,%s,%s,%s)",
            "INSERT INTO `mlusers` (`userid`) VALUES(%s)",
            "INSERT INTO `movie_actor` (`movieid`,`actorid`,`actor_movie_rank`) VALUES(%s,%s,%s)",
            "INSERT INTO `imdb_actor_info` (`actorid`,`name`,`gender`) VALUES(%s,%s,%s)",
            "INSERT INTO `genome_tags` (`tagid`,`tag`) VALUES(%s,%s)" ]

filenames = ['mltags.csv', 'mlmovies.csv','mlratings.csv','mlusers.csv','movie-actor.csv','imdb-actor-info.csv','genome-tags.csv']



def create_tables():
    for query in create_queries:
        try:
            cursor.execute(query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)

#loads table with given csv file
def insert_data(filename,query):
    csv_data = csv.reader(file(home_dir + filename))
    next(csv_data, None)

    print "Copying", filename
    for row in csv_data:
        cursor.execute(query, row)



create_tables()
for x,y in zip(filenames,insert_into_queries):
    insert_data(x,y)

#close the connection to the database.
mydb.commit()
cursor.close()
print "Db has been initialised"