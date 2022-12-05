import org.apache.spark.sql.SparkSession
import lib.ReadAndWrite
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}
import java.security.MessageDigest
import Configuration.Client



class Hash {


  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

  md5("Hello")



  def hashi(sparkSession: SparkSession, ds: Dataset[Client], id: Long): Dataset[Client] = {
    val encryptid = udf(md5 _)
    import sparkSession.implicits._

    val data : Dataset[Client] = ds
      .withColumn("IdentifiantClient", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("IdentifiantClient")))
      .withColumn("Nom", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Nom")))
      .withColumn("Prenom", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Prenom")))
      .withColumn("Adresse", when(col("IdentifiantClient") === id,  encryptid(col("IdentifiantClient").cast(StringType))).otherwise(col("Adresse")))
      .as[Client]
    data.show()
    data

  }

  def hashClient(sparkSession: SparkSession, id: Long): Unit = {
  println("Trying to hash data with id : " + id)
  val basePath = ""
  val data_path = "data"
  val data = ReadAndWrite.dataSetToRead(sparkSession, basePath + data_path).coalesce(1)
  val searchedID = data("IdentifiantClient") === id
  val result = data.filter(searchedID)
  result.show()
  val dataHashed = hashi(sparkSession, data, id)
  dataHashed.show()
  val resultHashed = dataHashed.filter(searchedID)
  resultHashed.show()
  ReadAndWrite.dataSetToWrite(basePath + "temp", dataHashed)
  }

}