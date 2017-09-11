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

'''will be stored locally to reduce sql queries
    size will be 1 - max(rank)'''
rank_weights = []
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


'''Helper function for computing temporal weights of timestamps and adding it into database
    should be called before performing any tasks'''
def compute_tag_weights():

    query = "SELECT * FROM information_schema.COLUMNS  WHERE  TABLE_SCHEMA = 'MWD_P1'  AND TABLE_NAME = 'mltags'  AND COLUMN_NAME = 'weight' "
    cursor.execute(query)
    ret = cursor.fetchall()
    if ret:
        print "Weights have already been computed"
        return

    query = "alter table mltags add weight float after timestamp"
    cursor.execute(query)
    cursor.fetchall()

    cursor.execute("select min(timestamp) from mltags")
    min_t = cursor.fetchall()[0][0]
    cursor.execute("select max(timestamp) from mltags")
    max_t = cursor.fetchall()[0][0]

    cursor.execute("select timestamp from mltags")
    ret = cursor.fetchall()
    times = [x[0] for x in ret]

    divider = (max_t - min_t).total_seconds()*1000  #milisec

    for t in times:
        '''
            weight = (cur - min) / (max-min)
            This values will be in [0,1] we will scale it to [0.5,1] using ax+b
             a=0.5, b=0.5
         '''
        delta = (t - min_t).total_seconds()*1000

        weight = 0.5*(delta/divider)+0.5
        cursor.execute("update mltags set weight=%s where timestamp=%s",(weight,t))

def compute_rank_weights():
    cursor.execute("select min(actor_movie_rank) from movie_actor")
    min_rank = cursor.fetchall()[0][0]
    cursor.execute("select max(actor_movie_rank) from movie_actor")
    max_rank = cursor.fetchall()[0][0]

    divider = (max_rank - min_rank)
    rank_weights = range(1,max_rank+2)
    r = 1
    for r in range(1,max_rank+1):
        '''
            weight = (cur - min) / (max-min)
            This values will be in [0,1] we will scale it to [0.5,1] using ax+b
             But here rank 1 gets 0 and highest one gets 1 ...so we will reverse it by assigning
                weight 1 to rank 1 (more imp)  - weight 0.5 to highest rank
             a= -0.5, b=1
         '''
        delta = r - min_rank
        weight = -0.5*(delta/float(divider))+1
        #print("Rank: ",r," weight",weight)
        rank_weights[r] = weight
    return rank_weights

'''Helper function that returns total weight: based on tag and actor weight'''
def get_weight(tag_w,rank_w):
    '''tag and rank weights are in range [0.5 - 1]
        we will add them and convert this combined [1 - 2] range back into [0.5,1]
        a=0.5 b=0'''
    return (tag_w+rank_w)*0.5+0
''' Performs logic of task 1  '''
def compute_TASK1(actorid,model):
    cursor.execute("select tagid,weight,actor_movie_rank from master where actorid=%s", (actorid,))
    ret = cursor.fetchall()

    tags = [x[0] for x in ret]
    weights = [x[1] for x in ret]
    ranks = [x[2] for x in ret]
    # print 'tags for actor: ',tags
    set_tags = set(tags)
    no_tags = len(tags)
    sum_tags = sum(weights) #instead of total count , sum of all tag weights
    tfidf = []
    tf= []

    cursor.execute("select count(actorid) from imdb_actor_info")
    no_actors = cursor.fetchall()[0][0]

    #compute tf-idf for each tag found for this actor
    for t in set_tags:
        # print 'For tag :',t,"occurance count: ",tag_count[0][0]
        '''Instead of normal tf counting where no_of_t will be used
            we will use sum_weights instead to give more emphasis to newer tags'''
        no_of_t = tags.count(t)
        '''find tags and corresponding weights'''
        start = -1
        sum_weights = 0
        while True:
            try:
                i = tags.index(t, start + 1)
            except ValueError:
                break
            else:
                sum_weights+= get_weight(weights[i],rank_weights[ranks[i]])
                start = i
        tf_ = sum_weights / sum_tags
        tf.append((t, tf_))
        if model == 'TF-IDF':
            cursor.execute("select count(DISTINCT actorid) from master where tagid=%s", (t,))
            actors_with_tag_t = cursor.fetchall()[0][0]
            idf = math.log(no_actors / actors_with_tag_t, 10)
            tf_idf = tf_ * idf;
            print 'tf:', tf_,' idf:',idf
            tfidf.append((t, tf_idf))
    if model == 'TF':
        print 'TF computed'
        return tf
    else:
        return tfidf

''' Performs logic of task 2  '''
def compute_TASK2(genre,model):
    cursor.execute("select tagid,weight from task2 where genre like %s", ('%'+genre+'%',))
    ret = cursor.fetchall()

    tags = [x[0] for x in ret]
    set_tags = set(tags)
    no_tags = len(tags)

    weights = [x[1] for x in ret]
    sum_tags = sum(weights)  # instead of total count , sum of all tag weights

    #total no of different genres
    cursor.execute("select count(DISTINCT genre) from mlmovies")
    no_genres = cursor.fetchall()[0][0]
    tf = []
    tfidf = []

    for t in set_tags:
        ''' Distinct because same movie,genre has different tags we need to avoid duplicate enris in this context
            We need only no of genres which hav been assigned this particular tag
            run this query without distinct and you will get to know'''
        #print 'For tag :',t,"occurance count: ",genre_with_tag_t
        '''Instead of normal tf counting where no_of_t will be used
            we will use sum_weights instead to give more emphasis to newer tags'''

        '''find tags and corresponding weights'''
        start = -1
        sum_weights = 0
        while True:
            try:
                i = tags.index(t, start + 1)
            except ValueError:
                break
            else:
                sum_weights += weights[i]
                start = i

        #no_of_t = tags.count(t)
        #tf_n = no_of_t / float(no_tags)
        #print 'tag:', t, ' Normal sum: ', no_of_t, ' weighted: ', sum_weights
        tf_ = sum_weights / sum_tags
        tf.append((t,tf_))
        if model == "TF-IDF":
            cursor.execute("select count(distinct genre) from task2 where tagid like %s", (t,))
            genre_with_tag_t = cursor.fetchall()[0][0]
            idf = math.log(no_genres / genre_with_tag_t, 10)
            tf_idf = tf * idf;
            print '\t weighted tf:', tf,' idf:',idf,' tf*idf = ',tf_idf
            print "tag: ",t,"  : ",tf_idf
            tf_idf.append((t, tf_idf))
        if model == 'TF':
            return tf
        else:
            return tfidf


compute_tag_weights()
rank_weights = compute_rank_weights()
#create_master_table()

''' Performs logic of task 1  '''
def compute_TASK3(userid,model):
    cursor.execute("select tagid,weight from mltags where userid=%s", (userid,))
    ret = cursor.fetchall()

    tags = [x[0] for x in ret]
    set_tags = set(tags)
    no_tags = len(tags)

    weights = [x[1] for x in ret]
    sum_tags = sum(weights)  # instead of total count , sum of all tag weights
    tag_weights = []

    #total no of different genres
    cursor.execute("select count(DISTINCT userid) from mlusers")
    no_users = cursor.fetchall()[0][0]
    tf = []
    tfidf = []

    for t in set_tags:
        ''' Distinct because same userid,tagid can be there for different movies we need to avoid duplicate entries in this context
            We need only no of users which have been assigned this particular tag (not the movies)
            run this query without distinct and you will get to know'''
        #print 'For tag :',t,"users count: ",users_with_tag_t
        '''Instead of normal tf counting where no_of_t will be used
            we will use sum_weights instead to give more emphasis to newer tags'''

        '''find tags and corresponding weights'''
        start = -1
        #instead of count(no_of_t) Sum of temporal weights for tag t for this user
        sum_weights = 0
        while True:
            try:
                i = tags.index(t, start + 1)
            except ValueError:
                break
            else:
                sum_weights += weights[i]
                start = i

        no_of_t = tags.count(t)
        tf_n = no_of_t / float(no_tags)
        print 'tag:', t, ' Normal sum: ', no_of_t, ' weighted: ', sum_weights
        tf_ = sum_weights / sum_tags
        tf.append((t,tf_))
        if model == 'TF-IDF':
            cursor.execute("select count(distinct userid) from mltags where tagid=%s", (t,))
            users_with_tag_t = cursor.fetchall()[0][0]
            idf = math.log(no_users / users_with_tag_t, 10)

            tf_idf = tf * idf;
            print '\tnormal tf:', (tf_n), ' weighted tf:', tf
            tfidf.append((t, tf_idf))
        if model == 'TF':
            return tf
        else:
            return tfidf

'''
#-------------- TASK 1  -----------------------
actor = int(raw_input('Enter actorid: '))
tag_weights = compute_TASK1(actor,'TF')

tag_weights.sort(key=lambda student: student[1], reverse=True)
print tag_weights

#-------------- TASK 2  -----------------------
genre = raw_input('Enter genre: ')
tag_weights = compute_TASK2(genre)
print tag_weights
'''
#-------------- TASK 3  -----------------------
'''
user = int(raw_input('Enter userid: '))
tag_weights = compute_TASK3(user)

tag_weights.sort(key=lambda student: student[1], reverse=True)
print tag_weights;
'''
#-------------- TASK 4  -----------------------

'''
print 'Finished computing!'
#cursor.execute("select tagid,timestamp from master where actorid=%s",(actor,))
#t = cursor.fetchall()

#close the connection to the database.
mydb.commit()
cursor.close()

print "Done"
'''
