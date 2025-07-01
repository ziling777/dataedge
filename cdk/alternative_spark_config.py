# 替代方案：使用 Maven 坐标而不是本地 JAR 文件
# 这样可以避免上传大文件的问题

spark_submit_parameters = (
    "--packages "
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.661,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.github.ben-manes.caffeine:caffeine:3.1.8,"
    "org.apache.commons:commons-configuration2:2.11.0 "
    "--conf spark.executor.cores=4 "
    "--conf spark.executor.memory=16g "
    f"--conf spark.sql.catalog.gpdemo=org.apache.iceberg.spark.SparkCatalog "
    f"--conf spark.sql.catalog.gpdemo.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog "
    f"--conf spark.sql.catalog.gpdemo.warehouse=arn:aws:s3tables:{Aws.REGION}:{Aws.ACCOUNT_ID}:bucket/caredgedemo "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    f"--conf spark.sql.catalog.defaultCatalog=gpdemo "
    f"--conf spark.sql.catalog.gpdemo.client.region={Aws.REGION}"
)

# 在 EMR 作业配置中使用这个配置，而不是引用本地 JAR 文件
