import pandas as pd
from sqlalchemy.engine import create_engine
import pymysql
import pandas as pd
from surprise import Dataset, Reader
from surprise.model_selection import train_test_split
from surprise import SVD
from surprise import Dataset
from surprise import accuracy
from surprise.model_selection import train_test_split


pymysql.install_as_MySQLdb()
engine = create_engine('mysql+pymysql://root:zx12zx12@localhost:3306/ml_chap3', convert_unicode=True)
conn = engine.connect()

ratings = pd.read_sql_table('ratings',conn)
movies = pd.read_sql_table('movie',conn)


ratings.columns = ['ratingid','userId', 'movieId','rating','timestamp']
movies.columns = ['movieId', 'title','a','b','c']
reader = Reader(rating_scale=(0.5, 5))

# load_from_df사용해서 데이터프레임을 데이터셋으로 로드
# 인자에 userid-itemid-ratings 변수들이 포함된 데이터프레임형태로 넣어주면 됨!
data = Dataset.load_from_df(ratings[['userId','movieId','rating']],
                           reader=reader)
train, test = train_test_split(data, test_size=0.25, random_state=42)

# 위에서 개별적으로 생성한 csv파일을 학습데이터로 생성
algo = SVD(n_factors=30, n_epochs=20, random_state=42)
algo.fit(train)



def get_unseen_surprise(ratings, movies, userId):
    # 특정 유저가 본 movie id들을 리스트로 할당
    seen_movies = ratings[ratings['userId'] == userId]['movieId'].tolist()
    # 모든 영화들의 movie id들 리스트로 할당
    total_movies = movies['movieId'].tolist()

    # 모든 영화들의 movie id들 중 특정 유저가 본 movie id를 제외한 나머지 추출
    unseen_movies = [movie for movie in total_movies if movie not in seen_movies]
    print(f'특정 {userId}번 유저가 본 영화 수: {len(seen_movies)}\n추천한 영화 개수: {len(unseen_movies)}\n전체 영화수: {len(total_movies)}')

    return unseen_movies


def recomm_movie_by_surprise(algo, userId, unseen_movies, top_n=10):
    # 알고리즘 객체의 predict()를 이용해 특정 userId의 평점이 없는 영화들에 대해 평점 예측
    predictions = [algo.predict(str(userId), str(movieId)) for movieId in unseen_movies]

    # predictions는 Prediction()으로 하나의 객체로 되어있기 때문에 예측평점(est값)을 기준으로 정렬해야함
    # est값을 반환하는 함수부터 정의. 이것을 이용해 리스트를 정렬하는 sort()인자의 key값에 넣어주자!
    def sortkey_est(pred):
        return pred.est

    # sortkey_est함수로 리스트를 정렬하는 sort함수의 key인자에 넣어주자
    # 리스트 sort는 디폴트값이 inplace=True인 것처럼 정렬되어 나온다. reverse=True가 내림차순
    predictions.sort(key=sortkey_est, reverse=True)
    # 상위 n개의 예측값들만 할당
    top_predictions = predictions[:top_n]

    # top_predictions에서 movie id, rating, movie title 각 뽑아내기
    top_movie_ids = [int(pred.iid) for pred in top_predictions]
    top_movie_ratings = [pred.est for pred in top_predictions]
    top_movie_titles = movies[movies.movieId.isin(top_movie_ids)]['title']
    # 위 3가지를 튜플로 담기
    # zip함수를 사용해서 각 자료구조(여기선 리스트)의 똑같은 위치에있는 값들을 mapping
    # zip함수는 참고로 여러개의 문자열의 똑같은 위치들끼리 mapping도 가능!
    top_movie_preds = [(ids, rating, title) for ids, rating, title in
                       zip(top_movie_ids, top_movie_ratings, top_movie_titles)]

    return top_movie_preds

def svd_kk(id):
    unseen_lst = get_unseen_surprise(ratings, movies, id)
    a = recomm_movie_by_surprise(algo, id, unseen_lst, top_n=10)
    print(a)


if __name__ =="main":
    svd_kk()