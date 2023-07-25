# Lo que intente hacer
Intente que el directorio scripts sea un paquete.
Intente con poner todos los modulos dentro del paquete utils en el mismo nivel que el ETL_market_charts.py y el error seguia siendo el mismo
Intente usar una variable de entorno de esta forma:
```Dockerfile
version: '3'
x-airflow-common:
  &airflow-common
  # ... (contenido previo del x-airflow-common)
  
services:
  postgres:
    # ... (contenido del servicio postgres)

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    environment:
      # Agregar aquí la variable de entorno PYTHONPATH apuntando al directorio que contiene el módulo transform_df.py
      - PYTHONPATH=/path/to/project/scripts/:$PYTHONPATH
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5

  airflow-scheduler:
    # ... (contenido del servicio airflow-scheduler)

  airflow-init:
    # ... (contenido del servicio airflow-init)

  spark:
    # ... (contenido del servicio spark)

  spark-worker:
    # ... (contenido del servicio spark-worker)

volumes:
  postgres-db-volume:
```

Sin embargo cuando ejecutaba `docker-compose up --build` el usuario por default `airflow` no existia ya que no me permitia entrar desde el inicio del `webserver`

Esto es todo lo que le pregunte a ChatGPT intentando solucionar el error de los logs que presento abajo:
https://chat.openai.com/share/bf7202ff-ddb2-4ddc-8541-082a398afa49


---
Dejo el LOG entero de la ultima vez que ejecute el DAG al dia de la fecha 25/07/2023


[2023-07-25, 20:22:09 UTC] {spark_submit.py:490} INFO - 23/07/25 20:22:09 INFO SharedState: Warehouse path is 'file:/opt/***/spark-warehouse'.
[2023-07-25, 20:22:25 UTC] {spark_submit.py:490} INFO - >>> Session de Spark creada.
[2023-07-25, 20:22:27 UTC] {spark_submit.py:490} INFO - >>> Solicitud de id:ethereum exitosa
[2023-07-25, 20:22:27 UTC] {spark_submit.py:490} INFO - >>> Solicitud de id:bitcoin exitosa
[2023-07-25, 20:22:27 UTC] {spark_submit.py:490} INFO - >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO A LA TRANSFORMACION
[2023-07-25, 20:23:44 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:44 INFO CodeGenerator: Code generated in 2551.051886 ms
[2023-07-25, 20:23:45 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:45 INFO CodeGenerator: Code generated in 422.651615 ms
[2023-07-25, 20:23:45 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:45 INFO CodeGenerator: Code generated in 70.392999 ms
[2023-07-25, 20:23:45 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:45 INFO CodeGenerator: Code generated in 70.14281 ms
[2023-07-25, 20:23:45 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:45 INFO CodeGenerator: Code generated in 81.15405 ms
[2023-07-25, 20:23:45 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:45 INFO CodeGenerator: Code generated in 79.792767 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 97.059561 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 55.328693 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 49.042906 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 62.319937 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 85.551521 ms
[2023-07-25, 20:23:46 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:46 INFO CodeGenerator: Code generated in 48.643599 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 98.969553 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 65.906093 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 56.852334 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 90.034861 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 64.964568 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 74.474315 ms
[2023-07-25, 20:23:47 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:47 INFO CodeGenerator: Code generated in 94.867729 ms
[2023-07-25, 20:23:48 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:48 INFO CodeGenerator: Code generated in 108.41962 ms
[2023-07-25, 20:23:48 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:48 INFO CodeGenerator: Code generated in 84.604986 ms
[2023-07-25, 20:23:48 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:48 INFO CodeGenerator: Code generated in 99.593512 ms
[2023-07-25, 20:23:48 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:48 INFO CodeGenerator: Code generated in 72.771861 ms
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO CodeGenerator: Code generated in 150.688415 ms
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 34 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 0
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 41 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 1
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 46 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 2
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 81 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 7
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 53 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 3
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 69 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 5
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 62 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 4
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Registering RDD 74 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 6
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Got job 0 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Final stage: ResultStage 8 (showString at NativeMethodAccessorImpl.java:0)
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2, ShuffleMapStage 3, ShuffleMapStage 7, ShuffleMapStage 4)
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 2, ShuffleMapStage 3, ShuffleMapStage 7, ShuffleMapStage 4)
[2023-07-25, 20:23:49 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:49 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[34] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:51 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:51 INFO AsyncEventQueue: Process of event SparkListenerJobStart(0,1690316629821,WrappedArray(org.apache.spark.scheduler.StageInfo@722cf941, org.apache.spark.scheduler.StageInfo@145b32c7, org.apache.spark.scheduler.StageInfo@5ade155e, org.apache.spark.scheduler.StageInfo@2ea3af07, org.apache.spark.scheduler.StageInfo@3bd0bd63, org.apache.spark.scheduler.StageInfo@76d06dd4, org.apache.spark.scheduler.StageInfo@5c8ef2b5, org.apache.spark.scheduler.StageInfo@6f66b5b4, org.apache.spark.scheduler.StageInfo@5163269e),{spark.driver.port=41939, spark.master=local[1], spark.submit.pyFiles=, spark.app.startTime=1690316510898, spark.executor.extraClassPath=/tmp/drivers/postgresql-42.5.2.jar, spark.rdd.compress=True, spark.executor.id=driver, spark.driver.extraClassPath=/tmp/drivers/postgresql-42.5.2.jar, spark.app.name=Spark y Redshift, spark.submit.deployMode=client, spark.driver.host=3bb72679b673, spark.app.id=local-1690316523902, __fetch_continuous_blocks_in_batch_enabled=true, spark.app.initial.jar.urls=spark://3bb72679b673:41939/jars/postgresql-42.5.2.jar, spark.sql.execution.id=0, spark.jars=/tmp/drivers/postgresql-42.5.2.jar, spark.sql.warehouse.dir=file:/opt/***/spark-warehouse, spark.serializer.objectStreamReset=100}) by listener AppStatusListener took 1.424523216s.
[2023-07-25, 20:23:52 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:52 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 18.8 KiB, free 434.4 MiB)
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.4 MiB)
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.4 MiB)
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[34] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
[2023-07-25, 20:23:53 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:53 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[41] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 18.8 KiB, free 434.4 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.4 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[41] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (3bb72679b673, executor driver, partition 0, PROCESS_LOCAL, 59822 bytes) taskResourceAssignments Map()
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO DAGScheduler: Submitting ShuffleMapStage 3 (MapPartitionsRDD[81] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 18.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.4 MiB)
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 3 (MapPartitionsRDD[81] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO TaskSchedulerImpl: Adding task set 3.0 with 1 tasks resource profile 0
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO DAGScheduler: Submitting ShuffleMapStage 4 (MapPartitionsRDD[53] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:54 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:54 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 18.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.4 MiB)
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 4 (MapPartitionsRDD[53] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO TaskSchedulerImpl: Adding task set 4.0 with 1 tasks resource profile 0
[2023-07-25, 20:23:55 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
[2023-07-25, 20:23:57 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO DAGScheduler: Submitting ShuffleMapStage 5 (MapPartitionsRDD[69] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 18.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.3 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.4 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 5 (MapPartitionsRDD[69] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO TaskSchedulerImpl: Adding task set 5.0 with 1 tasks resource profile 0
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO DAGScheduler: Submitting ShuffleMapStage 6 (MapPartitionsRDD[62] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:55 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 18.8 KiB, free 434.2 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:56 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 434.2 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:56 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 3bb72679b673:37595 (size: 8.9 KiB, free: 434.3 MiB)
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:56 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1388
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:56 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 6 (MapPartitionsRDD[62] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
[2023-07-25, 20:23:58 UTC] {spark_submit.py:490} INFO - 23/07/25 20:23:56 INFO TaskSchedulerImpl: Adding task set 6.0 with 1 tasks resource profile 0
[2023-07-25, 20:24:13 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:13 INFO CodeGenerator: Code generated in 222.402918 ms
[2023-07-25, 20:24:14 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:14 INFO CodeGenerator: Code generated in 258.096707 ms
[2023-07-25, 20:24:14 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:14 INFO CodeGenerator: Code generated in 211.309934 ms
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:16 INFO PythonRunner: Times: total = 10642, boot = 10429, init = 183, finish = 30
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:16 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:16 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:17 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (3bb72679b673, executor driver, partition 0, PROCESS_LOCAL, 59814 bytes) taskResourceAssignments Map()
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:17 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:17 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:17 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:17 ERROR TaskSetManager: Task 0 in stage 0.0 failed 1 times; aborting job
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO PythonRunner: Times: total = 56, boot = -4431, init = 4482, finish = 5
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 1)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:18 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage5.processNext(Unknown Source)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Cancelling stage 0
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage cancelled
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 2) (3bb72679b673, executor driver, partition 0, PROCESS_LOCAL, 75595 bytes) taskResourceAssignments Map()
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO Executor: Running task 0.0 in stage 3.0 (TID 2)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO DAGScheduler: ShuffleMapStage 0 (showString at NativeMethodAccessorImpl.java:0) failed in 28.290 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:19 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Cancelling stage 1
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage cancelled
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Stage 1 was cancelled
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 WARN TaskSetManager: Lost task 0.0 in stage 1.0 (TID 1) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage5.processNext(Unknown Source)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:20 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO DAGScheduler: ShuffleMapStage 1 (showString at NativeMethodAccessorImpl.java:0) failed in 24.553 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Cancelling stage 5
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 5: Stage cancelled
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Stage 5 was cancelled
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO DAGScheduler: ShuffleMapStage 5 (showString at NativeMethodAccessorImpl.java:0) failed in 23.451 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Cancelling stage 6
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 6: Stage cancelled
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Stage 6 was cancelled
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO DAGScheduler: ShuffleMapStage 6 (showString at NativeMethodAccessorImpl.java:0) failed in 22.932 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:21 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Cancelling stage 3
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 3: Stage cancelled
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO Executor: Executor is trying to kill task 0.0 in stage 3.0 (TID 2), reason: Stage cancelled
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Stage 3 was cancelled
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO DAGScheduler: ShuffleMapStage 3 (showString at NativeMethodAccessorImpl.java:0) failed in 24.441 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Cancelling stage 4
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Killing all running tasks in stage 4: Stage cancelled
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Stage 4 was cancelled
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO DAGScheduler: ShuffleMapStage 4 (showString at NativeMethodAccessorImpl.java:0) failed in 24.293 s due to Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (3bb72679b673 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:22 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:132)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.scheduler.Task.run(Task.scala:131)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - at java.base/java.lang.Thread.run(Thread.java:829)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - Driver stacktrace:
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO DAGScheduler: Job 0 failed: showString at NativeMethodAccessorImpl.java:0, took 29.876684 s
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - Traceback (most recent call last):
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/ETL_market_charts.py", line 74, in <module>
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO Executor: Executor killed task 0.0 in stage 3.0 (TID 2), reason: Stage cancelled
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - df = etl.transform(data)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/ETL_market_charts.py", line 57, in transform
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 WARN TaskSetManager: Lost task 0.0 in stage 3.0 (TID 2) (3bb72679b673 executor driver): TaskKilled (Stage cancelled)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:19 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - df = json_to_df_market_chart(data, self.spark)
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/opt/***/scripts/utils/transform_df.py", line 55, in json_to_df_market_chart
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - df.show()
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 484, in show
[2023-07-25, 20:24:23 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1305, in __call__
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 117, in deco
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - pyspark.sql.utils.PythonException:
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - An exception was thrown from the Python worker. Please see the stack trace below.
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - Traceback (most recent call last):
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 588, in main
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 447, in read_udfs
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 249, in read_single_udf
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - f, return_type = read_command(pickleSer, infile)
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 69, in read_command
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - command = serializer._read_with_length(file)
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 160, in _read_with_length
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - return self.loads(obj)
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - File "/home/***/.local/lib/python3.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 430, in loads
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - return pickle.loads(obj, encoding=encoding)
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - ModuleNotFoundError: No module named 'utils'
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:22 INFO SparkUI: Stopped Spark web UI at http://3bb72679b673:4040
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:22 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO MemoryStore: MemoryStore cleared
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO BlockManager: BlockManager stopped
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO BlockManagerMaster: BlockManagerMaster stopped
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO SparkContext: Successfully stopped SparkContext
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO ShutdownHookManager: Shutdown hook called
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO ShutdownHookManager: Deleting directory /tmp/spark-b10987ac-1c72-4b37-8d21-79caff2d4717/pyspark-2263eaa2-5977-439f-8f72-9b4907712f98
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:23 INFO ShutdownHookManager: Deleting directory /tmp/spark-07fc36e2-9ec4-44a2-841c-924dcc96a0e0
[2023-07-25, 20:24:24 UTC] {spark_submit.py:490} INFO - 23/07/25 20:24:24 INFO ShutdownHookManager: Deleting directory /tmp/spark-b10987ac-1c72-4b37-8d21-79caff2d4717
[2023-07-25, 20:24:24 UTC] {taskinstance.py:1824} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/providers/apache/spark/operators/spark_submit.py", line 157, in execute
    self._hook.submit(self._application)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/providers/apache/spark/hooks/spark_submit.py", line 422, in submit
    f"Cannot execute: {self._mask_cmd(spark_submit_cmd)}. Error code is: {returncode}."
airflow.exceptions.AirflowException: Cannot execute: spark-submit --master spark://spark:7077 --driver-class-path /tmp/drivers/postgresql-42.5.2.jar --name arrow-spark --queue default /opt/***/scripts/ETL_market_charts.py. Error code is: 1.
[2023-07-25, 20:24:24 UTC] {taskinstance.py:1350} INFO - Marking task as FAILED. dag_id=etl_market_chart, task_id=spark_etl_market_charts, execution_date=20230724T000000, start_date=20230725T202057, end_date=20230725T202424
[2023-07-25, 20:24:24 UTC] {standard_task_runner.py:109} ERROR - Failed to execute job 548 for task spark_etl_market_charts (Cannot execute: spark-submit --master spark://spark:7077 --driver-class-path /tmp/drivers/postgresql-42.5.2.jar --name arrow-spark --queue default /opt/***/scripts/ETL_market_charts.py. Error code is: 1.; 316)
[2023-07-25, 20:24:24 UTC] {local_task_job_runner.py:225} INFO - Task exited with return code 1
[2023-07-25, 20:24:25 UTC] {taskinstance.py:2653} INFO - 0 downstream tasks scheduled from follow-on schedule check