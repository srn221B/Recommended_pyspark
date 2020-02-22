iport psycopg2
import os
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext,functions as F
from pyspark.ml.recommendation import ALS
from sqlalchemy import create_engine

# データベースに接続するコネクションインスタンス生成関数
def get_connection():
	#　環境変数からデータベース接続情報を取得
	dsn = os.environ['DATABASE_URL']
	#　コネクションインスタンスを返す
	return psycopg2.connect(dsn)

# SparkSQL利用するためのインスタンス生成関数
def get_sqlcontext():
	#  SparkContextインスタンスの生成
	sc = SparkContext("local", "App Name")
	#  SQLContextインスタンスの生成
	sql_context = SQLContext(sc)
	#  SQLContextインスタンスを返却
	return sql_context

#  ユーザーのレビューDataFrame生成関数
def create_dataframe_reviews(sql,con):
	#　 Reviews上のデータをpandasに持っていく（引数　コネクションインスタンス）
	data = pd.read_sql(sql='SELECT * from "Reviews";',con=con)
	#  pandas上のデータをSpark用のDataFrameに変更
	sdf = sql.createDataFrame(data)
	#  ユーザーレビューDataFrameを返却
	return sdf

#　ユーザーリスト生成関数
def create_dataframe_users(con):
	#　レビューしているユーザーのidを抽出しpandasに持っていく（引数　コネクションインスタンス）
	data = pd.read_sql(sql='select distinct(user_name_id) from "Reviews";',con=con)
	#　dataをリスト型にする
	user_list = data.values.tolist()
	#　ユーザーリストを返却
	return user_list

# ALS学習model生成関数
def learning_model(schema):
	#  ALSトレーニングデータの生成
	als = ALS(
		rank=30,
		maxIter=10,
		regParam=0.1,
		userCol='user_name_id',
		itemCol='contents_id',
		ratingCol='review',
		seed=0
	)
	#  学習を行いmodelに出力
	model = als.fit(schema)
	#　モデルの返却
	return model

# モデルをRDB上に登録関数
def Add_data(model,conn,user_list):

	# 現在あるRDB上のratingデータを削除
	cur = conn.cursor()
	cur.execute("DELETE FROM review_rating")
	conn.commit()

	# pandasからRDBに接続するためのengine生成
	engine = create_engine(os.environ['DATABASE_URL'])

	# ユーザーごとにreview_ratingに登録
	for user in user_list:
		#　ユーザーごとのおすすめ作品を２０個取り出す
		#  Row(contents_id,rating)で返却
		df1 = model.recommendForAllUsers(
			numItems=30
		).filter(
			'user_name_id = %d' % user[0]
		).select(
			'recommendations.contents_id',
			'recommendations.rating'
		).first()

		#　pandasのDataframeに変換
		recommends = pd.DataFrame({
			'user' : user[0],
			'content' : df1[0],
			'rating' : df1[1]
		})

		#  RDB上に登録
		recommends.to_sql('review_rating',engine,if_exists='append',index=False)



# データベースに接続するコネクションインスタンス生成関数を呼び出す
conn = get_connection()
# SparkSQL利用するためのインスタンス生成関数を呼び出す
sqlcontext = get_sqlcontext()
# ユーザーのレビューDataFrame生成関数を呼び出す
schema = create_dataframe_reviews(sqlcontext,conn)
# ユーザーリスト生成関数を呼び出す
user_list = create_dataframe_users(conn)
# ALS学習model生成関数を呼び出す
model = learning_model(schema)
# モデルをRDB上に登録関数
Add_data(model,conn,user_list)
