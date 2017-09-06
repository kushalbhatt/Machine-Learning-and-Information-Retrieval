import csv
import mysql.connector
from mysql.connector import errorcode
import math

mydb = mysql.connector.connect(host='localhost',
    user='root',
    passwd='padfoot',
    db='MWD_P1')

cursor = mydb.cursor(buffered=True)

query= "CREATE TABLE `mlusers` (`userid` INT)"
'''
try:
    cursor.execute(query)
except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
else:
    print("Table Created")

csv_data = csv.reader(file('/home/kushalbhatt/ASU/ASU/MWD/phase1_dataset/imdb-actor-info.csv'))
next(csv_data,None)

for row in csv_data:
    cursor.execute("INSERT INTO `imdb_actor_info` (`actorid`,`name`,`gender`) VALUES(%s,%s,%s)", row);
    #cursor.execute("INSERT INTO `genome_tags` (`tagid`,`tag`) VALUES(%s,%s)",row);
'''
cursor.execute("select actorid from imdb_actor_info")
ret = cursor.fetchall()
actors = [x[0] for x in ret]
no_actors = len(actors)	
done = 0
''' Find tag weights for all actors'''
for actor in actors:
	#actor = int(raw_input('Enter actorid: '))
	cursor.execute("select tagid from master where actorid=%s",(actor,))
	ret = cursor.fetchall()

	tags = [x[0] for x in ret]
	#print 'tags for actor: ',tags
	tag_count = []
	set_tags = set(tags)
	no_tags = len(tags)  #counting only distinct ones
	tag_weights = []
	for t in set_tags:
		cursor.execute("select count(actorid) from master where tagid=%s",(t,))
		tag_count = cursor.fetchall()
		#print 'For tag :',t,"occurance count: ",tag_count[0][0]
		no_of_t = tags.count(t)
		#print '\t',no_of_t,' tags:', no_tags,'\tactors: ',no_actors,' ,acors with this tag: ',tag_count[0][0] 	
		tf = no_of_t / float(no_tags)
		idf = math.log(no_actors / tag_count[0][0],10)
		weight = tf * idf ;
		tag_weights.append((t,weight))
		#print '<',t,',',weight,'>'
	done = done + 1
	if done % 10 == 0:
		print done,' . . .'
	#tag_weights.sort(key=lambda student: student[1], reverse=True)
	#print tag_weights;

print 'Finished computing!'
#cursor.execute("select tagid,timestamp from master where actorid=%s",(actor,))
#t = cursor.fetchall()








#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"
