import json

import pymysql
from flask import Flask, render_template
from flask import request
from reco import recommend
from svg import recomm_movie_by_surprise

from flask import jsonify

# 유저정보 출력
def userInfo(id):
    ret = []
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql= '''select * from user where userId = %s;'''
    cursor.execute(sql,(id))
    rows = cursor.fetchall()
    for e in rows:
        temp = {'userId': e[0], 'age': e[1], 'gender': e[2], 'occupation':e[3], 'zipcode': e[4]}
        ret.append(temp)
    db.commit()
    db.close()
    return ret

# 같은 연령대 top 10
def age_rating(id):
    ret = []
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql = '''
    select MovieTitle,avg(ratingScore) from ratings natural join user natural join movie
    where 
    floor((select age from user where userId = %s)/10)*10 <= age && age < ceiling((select age from user where userId = %s)/10)*10
    group by movieId
    order by avg(ratingScore) desc limit 10;
    '''
    cursor.execute(sql,(id,id))
    rows = cursor.fetchall()
    for e in rows:
        temp = {'title': e[0], 'ratings': e[1]}
        ret.append(temp)

    db.commit()
    db.close()
    return ret

#같은 성별 top 10
def sex_rating(id):
    ret = []
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql = '''
    select MovieTitle,avg(ratingScore) from ratings natural join user natural join movie
    where gender = (select gender from user where userId = %s)
    group by movieId
    order by avg(ratingScore) desc limit 10;
    '''
    cursor.execute(sql,(id))
    rows = cursor.fetchall()
    for e in rows:
        temp = {'title': e[0], 'ratings': e[1]}
        ret.append(temp)

    db.commit()
    db.close()
    return ret
#직무별 top 10 // json 형태로 리턴
def occupation(id):
    ret = []
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql = '''
    select MovieTitle,avg(ratingScore) 
    from ratings natural join user natural join movie
    where occupation = any(select occupation from user where userId = %s)
    group by movieId
    order by avg(ratingScore) desc limit 10;
    '''
    cursor.execute(sql,(id))
    rows = cursor.fetchall()
    for e in rows:
        temp = {'title': e[0], 'ratings': e[1]}
        ret.append(temp)

    db.commit()
    db.close()
    return ret
    # cursor.execute(sql,(id))
    # result = cursor.fetchall()
    # db.close()
    # return str(result)

#회원이 평점을 내린 리스트 추출
def ratings(id):
    ret = []
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql = '''
    select MovieTitle,avg(ratingScore) from ratings natural join user natural join movie
    where userId = %s
    group by movieId
    order by avg(ratingScore) desc'''
    cursor.execute(sql,(id))
    rows = cursor.fetchall()
    for e in rows:
        temp = {'title': e[0], 'ratings': e[1]}
        ret.append(temp)

    db.commit()
    db.close()
    return ret
def db_connector3(id):
    db = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='zx12zx12',
        db='ml_chap3',
        charset='utf8'
    )
    cursor = db.cursor()
    sql = '''
    select * from user
    where userId = %s
    '''
    cursor.execute(sql,(id))
    rows = cursor.fetchall()
    temp = {'Id':rows[0],'age':rows[1]}
    # result = cursor.fetchall()
    db.close()
    return str(temp)
    # return str(result)

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("test.html")

@app.route('/test2', methods=['GET'])
def test_get2():
   title_receive = request.args.get('title_give')
   a = db_connector3(title_receive)
   print(title_receive)
   return a
   # return jsonify({'result': title_receive, 'msg': '이 요청은 GET!'})

@app.route('/login', methods=['GET'])
def login():
   # val = request.args.get('id', type=int)
   title_receive = request.args.get('id')
   id = userInfo(title_receive)
   a = occupation(title_receive)
   b = ratings(title_receive)
   re = recommend(int(title_receive),10)
   age = age_rating(title_receive)
   # svd = recomm_movie_by_surprise(int(title_receive))
   k = sex_rating(title_receive)
   if not id:
       return render_template('movie.html')
   return render_template('movie.html',id= id, a=a,b=b, title_receive=title_receive, re=re, k = k,age= age,svd= svd)

@app.route('/user', methods=['GET'])
def userfind():
   title_receive = request.args.get('title_give')
   a = userInfo(title_receive)

   print(a[1])
   return a
if __name__ == "__main__":

    app.run(host="0.0.0.0", port="8080")
