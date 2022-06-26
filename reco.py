import pandas as pd
import numpy as np
from sqlalchemy.engine import create_engine
import pymysql
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors
from scipy import spatial
import operator


pymysql.install_as_MySQLdb()
engine = create_engine('mysql+pymysql://root:zx12zx12@localhost:3306/ml_chap3', convert_unicode=True)
conn = engine.connect()

df_ratings = pd.read_sql_table('ratings',conn)
df_movies = pd.read_sql_table('movie',conn)
# print(df_ratings.head())
# print(df_ratings.columns)
# print(df_movies.head())
# print(df_ratings.columns)


df_ratings.drop('timestamp', axis=1, inplace=True)
df_ratings.drop('ratingId', axis=1, inplace=True)
df_movies.drop('releaseDate', axis=1, inplace=True)
df_movies.drop('videoReleaseDate', axis=1, inplace=True)
df_movies.drop('IMDbURL', axis=1, inplace=True)
# print(df_ratings.head())
# print(df_movies.head())

df_genres = pd.read_sql_table('movie_genres',conn)
df_genres.drop('mgenreId', axis=1, inplace=True)
df_k = pd.merge(df_genres,df_movies, on ='movieId')

df = pd.merge(df_ratings,df_movies, on ='movieId')

df_movie_features = df_ratings.pivot(
    index='movieId',
    columns='userId',
    values='ratingScore'
).fillna(0)

# convert dataframe of movie features to scipy sparse matrix
mat_movie_features = csr_matrix(df_movie_features.values)

model_knn = NearestNeighbors(metric='cosine', algorithm='brute', n_neighbors=20, n_jobs=-1)


movieProperties = df.groupby('movieId').agg({'ratingScore':[np.size,np.mean]})
movieNumRatings = pd.DataFrame(movieProperties['ratingScore']['size'])
movieNormalizedNumRatings = movieNumRatings.apply(lambda x:(x-np.min(x))/(np.max(x)-np.min(x)))

# print(movieProperties.head())

movieDict = {}

for index, row in df_movies.iterrows():
    movieID = int(row['movieId'])
    name = row['movieTitle']
    genres = list(row[2:])
    movieDict[movieID] = (name, np.array(list(genres)), movieNormalizedNumRatings.loc[movieID].get('size'),
    movieProperties.loc[movieID].ratingScore.get('mean'))

print(movieDict[1])



# 장르, 인기도의 consine 유사도 적용

def ComputeDistance(a, b):
    genresA = a[1]
    genresB = b[1]
    genreDistance = spatial.distance.cosine(genresA, genresB)
    popularityA = a[2]
    popularityB = b[2]
    popularityDistance = abs(popularityA - popularityB)
    return genreDistance + popularityDistance


ComputeDistance(movieDict[1], movieDict[4])


# neighbors 출력
def getNeighbors(movieID, K):
    distances = []
    for movie in movieDict:
        # 같은 movie가 아닐때만 movie distance를 구함
        if (movie != movieID):
            dist = ComputeDistance(movieDict[movieID], movieDict[movie])
            distances.append((movie, dist))
    # movie distance를 sort시켜주어 가장 가까운 영화들을 추천
    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    for x in range(K):
        neighbors.append(distances[x][0])
    return neighbors


# 최종 추천
def recommend(movieID, K):
    avgRating = 0
    print(movieDict[movieID], '\n')
    ret = []
    neighbors = getNeighbors(movieID, K)  # Toy Story (1995)
    for neighbor in neighbors:
        # neigbor의 평균 rating을 더해줌
        avgRating += movieDict[neighbor][3]
        temp = {'title': movieDict[neighbor][0]}
        ret.append(temp)
        # print(movieDict[neighbor][0] + " " + str(movieDict[neighbor][3]))
    avgRating /= K
    # print("평균 Rating: ", avgRating)
    # print(type(ret))
    return ret

# a = recommend(1, 30)

# def export_model(a):
#     recommend(a,30)
if __name__ =="main":
    recommend()