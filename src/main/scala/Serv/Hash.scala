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


  //fonction de hashage md5, prend un string retourne un hash correspondant
  def md5(s: String): BigInteger = {
    new java.math.BigInteger(
      MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))
  }



  def hashi(sparkSession: SparkSession, ds: Dataset[Client], id: Long): Dataset[Client] = {
    val encryptid = udf(md5 _)


    import sparkSession.implicits._
    val data : Dataset[Client] = ds

//passe sur chaque valeur de la colonne "Nom" verifie si la ligne correspond a celle de l'id indique,
      .withColumn("Nom", when(col("IdentifiantClient") === id,

// si la valeur correpond ca remplace la valeur de la case par celle du hash genere
      encryptid(col("IdentifiantClient").cast(StringType)))

//si non, ca laisse la valeur initiale
      .otherwise(col("Nom")))

      .withColumn("Prenom", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Prenom")))
      .withColumn("Adresse", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Adresse")))
      .as[Client]

//ici on a fait le choix de ne pas hasher les id et date de souscription pour qu'on puisse retrouver les valeurs hashées
    println("je suis dans hashi")
    data.show()
    data

  }

  def hashClient(sparkSession: SparkSession, id: Long): Unit = {

  val hdfs_path = "hdfs://172.31.254.20:9090/user/bronze/json/"
    //ici, la path est celui de la ou on vas lire le csv
  val data = ReadAndWrite.dataSetToRead(sparkSession, "csv" /*hdfs_path + "hashedFor2564888"*/).coalesce(1)
  val dataHashed = hashi(sparkSession, data, id)
  println("dataHashed")
  dataHashed.show()
    //ici, la path est celui de la ou on vas lire le csv
    ReadAndWrite.dataSetToWrite(hdfs_path + "/v2", dataHashed)
  }

}