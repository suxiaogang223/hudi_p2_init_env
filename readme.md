# hudi_p2_init_env
shells to init the emr environment for hudi p2 case

## quick start
```bash
python3 -m pip install -r requirements.txt
# init user_activity_log tables
python3 init_hudi_tables.py --batch_size 1000 --batch_num 10
# init hudi schema change tables
python3 init_hudi_schema_evolution_tables.py
# init hudi timestamp tables
spark-sql --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' -f init_hudi_timestamp_tables.sql
# init hudi partition pruning tables
spark-sql --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' -f init_hudi_partition_pruning_tables.sql
```