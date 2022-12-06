package Serv

import Configuration.Client
import lib.ReadAndWrite
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.StringType
import java.math.BigInteger
import java.security.MessageDigest


object Delete {


  def delete(dataset: Dataset[Client], id: Long): Dataset[Client] = {
    val clean = dataset.filter(!col("IdentifiantClient").isin(id))
    clean
  }



  def deleteById(sparkSession: SparkSession, id: Long): Unit = {
    val sparkSession = SparkSession.builder().appName("dataproject-Amal-Kinan").master("local").getOrCreate()


    val hdfs_path = "hdfs://172.31.254.20:9090/user/bronze/json/"
    val dataset = ReadAndWrite.dataSetToRead(sparkSession, "csv" /*hdfs_path + "hashedFor2564888/"*/ )
    dataset.show()
    dataset.printSchema()
    val clean = delete(dataset, id)
    clean.show()
    ReadAndWrite.dataSetToWrite(hdfs_path + "v2" , clean)
  }

}