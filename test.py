import scipy
import scipy.sparse as sp
from scipy.sparse.linalg import svds
import pandas as pd
import numpy as np
from core import *
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.decomposition import LatentDirichletAllocation as LDA

mydb = mysql.connector.connect(host='localhost',
    user='root',
    passwd='padfoot',
    db='mwd')
cursor = mydb.cursor(buffered=True)

cursor.execute("select distinct(genre) from master");
ret = cursor.fetchall()
genres = [i[0] for i in ret]

cursor.execute("select distinct(tagid) from master")
ret = cursor.fetchall()
tags = [x[0] for x in ret]

'''Here the data is genre X actors with each cell having Tf-IDF values for that genre and actor'''
def compute_Semantics_actor(method, genre):
    cursor.execute("select distinct(actorid) from master");
    ret = cursor.fetchall()
    actors = [i[0] for i in ret]

    if (genre not in genres):
        print "Genre doesn't exist in the dataset!"
        return

    '''Matrix Dataset'''
    V = sp.lil_matrix((len(genres), len(actors)))

    '''get tf-idfs vectors for each genre w.r.t actors'''
    count = 0
    for i in range(len(genres)):
        g = genres[i]
        # tf_idf = compute_tf_idf_movie(cur_movie,"TF-IDF")
        tf_idf = compute_tf_idf_actor_genre(g)
        for j in range(len(actors)):
            cell = [0]
            if actors[j] in [entry[0] for entry in tf_idf]:
                cell = [a[1] for a in tf_idf if a[0] == actors[j]]
                # print "found tag",tags[i],": ",cell
            V[i, j] = cell[0]

    if (method == 'SVD'):
        '''  SVD  Calculation '''
        U, sigma, Vt = svds(V, k=4)
        sigma = np.diag(sigma)
        # print "\n\nSigma = \t",sigma
        print "\n\nU:", len(U), len(U[0]), "Sigma: ", sigma.shape, " V: ", Vt.shape, "\n\n"
        print U
        print "For genre Latent semantics are:", U[genres.index(genre)]

    if (method == 'PCA'):
        # standardizing data
        V = sp.csr_matrix(V).todense()
        V_std = StandardScaler().fit_transform(V)
        print "Stdandardized size: ", V_std.shape

        '''PCA::   Using Inbuilt library function'''
        sklearn_pca = PCA(n_components=4)
        pca = sklearn_pca.fit_transform(V_std)
        print "\n\n\n PCA \n", pca.shape, pca


def compute_Semantics_genre(method, genre):
    if(genre not in genres):
        print "Genre doesn't exist in the dataset!"
        return
    '''Matrix Dataset'''
    V = sp.lil_matrix((len(genres), len(tags)))

    '''get tf-idfs vectors for movies and fill the'''
    count = 0
    for i in range(len(genres)):
        g = genres[i]
        # tf_idf = compute_tf_idf_movie(cur_movie,"TF-IDF")
        tf_idf = compute_TASK2(g, "TF-IDF")
        for j in range(len(tags)):
            cell = [0]
            if tags[j] in [tag[0] for tag in tf_idf]:
                cell = [t[2] for t in tf_idf if t[0] == tags[j]]
                # print "found tag",tags[i],": ",cell
            V[i, j] = cell[0]

    if(method == 'SVD'):
        '''  SVD  Calculation '''
        U, sigma, Vt = svds(V, k=4)
        sigma = np.diag(sigma)
        # print "\n\nSigma = \t",sigma
        print "\n\nU:", len(U), len(U[0]), "Sigma: ", sigma.shape, " V: ", Vt.shape, "\n\n"
        print U
        print "For genre Latent semantics are:", U[genres.index(genre)]

    if(method == 'PCA'):
        # standardizing data
        V = sp.csr_matrix(V).todense()
        V_std = StandardScaler().fit_transform(V)
        print "Stdandardized size: ", V_std.shape

        '''PCA::   Using Inbuilt library function'''
        sklearn_pca = PCA(n_components=4)
        pca = sklearn_pca.fit_transform(V_std)
        print "\n\n\n PCA \n",pca.shape,pca
    if(method == 'LDA'):
        '''TO:DO://  Create matrix with doc as rows and words as column s with each cell having freq count not tf-idf'''
        for i in range(len(genres)):
            g = genres[i]
            cursor.execute("select tagid,weight from task2 where genre like %s", ('%' + g + '%',))
            ret = cursor.fetchall()
            tags_in_genre = [x[0] for x in ret]

            for j in range(len(tags)):
                cell = tags_in_genre.count(tags[j])
                V[i,j]=cell

        lda = LDA(n_components=5,max_iter=20,learning_method="batch")
        ans = lda.fit_transform(V)
        print ans.shape,"\n",ans,"\n\n\n"
compute_Semantics_genre("PCA","Drama")
#compute_Semantics_actor("SVD","Action")
#compute_Semantics_genre("LDA","Action")
mydb.commit()
cursor.close()
# #print V_std
# '''co-variance matrix'''
# mean_vec = np.mean(V_std, axis=0)
# #print "\nMean : \n",mean_vec
# #print "Mean vector: ",len(mean_vec),len(mean_vec[0]),mean_vec.shape
#
# cov_mat = np.cov(V_std.T.astype(float))
# cov_mat = (V_std - mean_vec).T.dot((V_std - mean_vec)) / (V_std.shape[0]-1)
# print "Covariance: ", len(cov_mat),len(cov_mat[0])
#
# eig_vals, eig_vecs = np.linalg.eig(cov_mat)
# #print "Normal way: Eig vec = ",eig_vecs
#
# '''(Alternatively) fro PCA computation we can USE SVD as well by not doing covariance,eigenvector stuff by ourselves'''
# U,sigma,Vt = svds(V_std.T)
# #print "how abouth this? with SVD: \n",U