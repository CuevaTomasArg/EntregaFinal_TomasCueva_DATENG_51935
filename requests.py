[('bitcoin', {
    'prices': [        [1664486400000, 32000.0],
        [1664572800000, 31800.0],
        [1664659200000, 31000.0],
        [1664745600000, 30500.0],
        [1664832000000, 31200.0],
        [1664918400000, 30500.0],
        [1665004800000, 31000.0]
    ],
    'market_caps': [        [1664486400000, 600000000000.0],
        [1664572800000, 580000000000.0],
        [1664659200000, 550000000000.0],
        [1664745600000, 520000000000.0],
        [1664832000000, 560000000000.0],
        [1664918400000, 530000000000.0],
        [1665004800000, 540000000000.0]
    ],
    'total_volumes': [        [1664486400000, 10000000000.0],
        [1664572800000, 9000000000.0],
        [1664659200000, 8000000000.0],
        [1664745600000, 7000000000.0],
        [1664832000000, 7500000000.0],
        [1664918400000, 6000000000.0],
        [1665004800000, 8000000000.0]
    ]
}),
 ('ethereum', {
    'prices': [        [1664486400000, 32000.0],
        [1664572800000, 31800.0],
        [1664659200000, 31000.0],
        [1664745600000, 30500.0],
        [1664832000000, 31200.0],
        [1664918400000, 30500.0],
        [1665004800000, 31000.0]
    ],
    'market_caps': [        [1664486400000, 600000000000.0],
        [1664572800000, 580000000000.0],
        [1664659200000, 550000000000.0],
        [1664745600000, 520000000000.0],
        [1664832000000, 560000000000.0],
        [1664918400000, 530000000000.0],
        [1665004800000, 540000000000.0]
    ],
    'total_volumes': [        [1664486400000, 10000000000.0],
        [1664572800000, 9000000000.0],
        [1664659200000, 8000000000.0],
        [1664745600000, 7000000000.0],
        [1664832000000, 7500000000.0],
        [1664918400000, 6000000000.0],
        [1665004800000, 8000000000.0]
    ]
})]

[2023-07-23, 12:14:05 UTC] {spark_submit.py:490} INFO - >>> Session de Spark creada.
[2023-07-23, 12:14:06 UTC] {spark_submit.py:490} INFO - >>> Solicitud de id:ethereum exitosa
[2023-07-23, 12:14:06 UTC] {spark_submit.py:490} INFO - >>> Solicitud de id:bitcoin exitosa
[2023-07-23, 12:14:06 UTC] {spark_submit.py:490} INFO - >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO A LA TRANSFORMACION
[2023-07-23, 12:14:06 UTC] {spark_submit.py:490} INFO - >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO AL MODULO json_to_df_market_chart
[2023-07-23, 12:14:06 UTC] {spark_submit.py:490} INFO - >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO AL MODULO PROCESS_DATA
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - Traceback (most recent call last):
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/ETL_market_charts.py", line 74, in <module>
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - df = etl.transform(data)
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/ETL_market_charts.py", line 57, in transform
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - df = json_to_df_market_chart(data, self.spark)
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/utils/transform_df.py", line 43, in json_to_df_market_chart
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - dfs = list(map(process_data, data_cleaned))
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/utils/transform_df.py", line 38, in process_data
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - df_final = df_final.select("id", "prices", "total_volumes", "volumes", "date_unix")
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 1669, in select
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1305, in __call__
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 117, in deco
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - pyspark.sql.utils.AnalysisException: cannot resolve '`id`' given input columns: [date_unix, market_caps, prices, total_volumes];
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - 'Project ['id, prices#1, total_volumes#13, 'volumes, date_unix#16L]
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - +- Project [coalesce(date_unix#8L, date_unix#12L) AS date_unix#16L, prices#1, market_caps#5, total_volumes#13]
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - +- Join FullOuter, (date_unix#8L = date_unix#12L)
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - :- Project [coalesce(date_unix#0L, date_unix#4L) AS date_unix#8L, prices#1, market_caps#5]
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - :  +- Join FullOuter, (date_unix#0L = date_unix#4L)
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - :     :- LogicalRDD [date_unix#0L, prices#1], false
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - :     +- LogicalRDD [date_unix#4L, market_caps#5], false
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - +- LogicalRDD [date_unix#12L, total_volumes#13], false
[2023-07-23, 12:14:20 UTC] {spark_submit.py:490} INFO - 