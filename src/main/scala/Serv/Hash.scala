package Serv

import org.apache.spark.sql.SparkSession
import lib.ReadAndWrite
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

import java.security.MessageDigest
import Configuration.Client
import org.apache.spark.sql.sources.EqualTo

import java.math.BigInteger



object Hash {


  def md5(s: String): BigInteger = {
    new java.math.BigInteger(
      MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))
  }



  def hashi(sparkSession: SparkSession, ds: Dataset[Client], id: Long): Dataset[Client] = {
    val encryptid = udf(md5 _)


    import sparkSession.implicits._

    val data : Dataset[Client] = ds
      .withColumn("Nom", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Nom")))
      .withColumn("Prenom", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Prenom")))
      .withColumn("Adresse", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Adresse")))
      .as[Client]
    println("je suis dans hashi")
    data.show()
    data

  }

  def hashClient(sparkSession: SparkSession, id: Long): Unit = {

  val hdfs_path = "hdfs://172.31.254.20:9090/user/bronze/json/"
  val path = "data"
  val data = ReadAndWrite.dataSetToRead(sparkSession, path).coalesce(1)
  val searchedID = data("IdentifiantClient") === id
  val dataHashed = hashi(sparkSession, data, id)
  println("dataHashed")
  dataHashed.show()
  val resultHashed = dataHashed.filter(searchedID)
  resultHashed.show()
  ReadAndWrite.dataSetToWrite(hdfs_path + "/hashedFor" + id, dataHashed)
  }

}