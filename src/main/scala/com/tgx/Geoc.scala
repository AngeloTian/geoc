package com.tgx

import org.apache.spark.sql.SparkSession

object Geoc {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("geoc").master("local[2]").getOrCreate()

    //inferReflection(spark)

    program(spark)

    spark.stop()
  }

  def program(spark: SparkSession): Unit = {
    val spark = SparkSession.builder().appName("GeocAp")
      .master("local[2]").getOrCreate()


    /**
      * spark.read.format("parquet").load 这是标准写法
      */
    val userDF = spark.read.format("parquet").
      load("hdfs://localhost:8020/user/hive/warehouse/dwd.db/dwd_bill_deviceinfo_offline_position_d")

    userDF.printSchema()
    userDF.show()

    //    userDF.select("gps_lon", "gpt_lat").show
    //
    //    userDF.select("name", "favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout")
    //
    //    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").show
    //
    //    会报错，因为sparksql默认处理的format就是parquet
    //    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").show
    //
    //    spark.read.format("parquet").option("path", "file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load().show
    spark.stop()
  }
}
