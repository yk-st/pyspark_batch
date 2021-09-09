#!/usr/bin/env python
# coding: utf-8
import sys
import argparse
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# parser
class ParamProcessor(argparse.Action):
    """
    --param foo=a型の引数を辞書に入れるargparse.Action
    """
    def __call__(self, parser, namespace, values, option_strings=None):
        param_dict = getattr(namespace,self.dest,[])
        if param_dict is None:
            param_dict = {}

        k, v = values.split("=")
        param_dict[k] = v
        setattr(namespace, self.dest, param_dict)

def main():

    parser = argparse.ArgumentParser(description='args')
    parser.add_argument('-a', '--appName', type=str, required=True, help='application name displaied at spark history server')
    parser.add_argument('-p', '--path', type=str, required=True, help='application name displaied at spark history server')
    parser.add_argument('-o', '--outpath', type=str, required=True, help='application name displaied at spark history server')
    parser.add_argument('-s', '--sql', type=str, required=False, help='specify sql path')
    parser.add_argument('-b', '--sqlBinding', required=False, help='sql placeholder ex. --sqlBinding dt=2019-01-01 --sqlBinding action=show',action=ParamProcessor)
    parser.add_argument('-t', '--tableName', type=str, required=False, help='specify sql path')
    
    args = parser.parse_args()
    parser=argparse.ArgumentParser()

    # generate spark session
    spark = SparkSession.builder \
    .appName("chapter3") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.session.timeZone", "JST") \
    .config("spark.ui.enabled","true") \
    .config("spark.eventLog.enabled","true") \
    .enableHiveSupport() \
    .getOrCreate()

    print(args)

    #SQLを読み込む
    # S3などのオブジェクトストレージでも大丈夫です
    df=spark.read.option("encoding", "utf-8").text(args.sql)
    query=' '.join([str(x.asDict()['value']) for x in sql.collect()])

    # プレースホルダーに値をセットする
    if args.sqlBinding is not None:
      query=query.format(**args.sqlBinding)

    print(query)
    
    #　テンポラリテーブルを作成する
    df.createOrReplaceTempView(args.targetTable)

    # SQLを発行する(今回はinsert 文)
    #etl=spark.sql(query)

    #以降にdataframeの処理を書いてももちろん問題なしです。

    spark.stop()

if __name__ == '__main__':
    main()
