import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import Configuration.Client

object Main {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("dataproject-Amal-Kinan")
      .master("local")
      .getOrCreate()


    deleteById(sparkSession: SparkSession)

  }

  def delete(dataset: Dataset[Client], id: Long): Dataset[Client] = {
    val clean = dataset.filter(!col("IdentifiantClient").isin(id))
    clean
  }

  def dataSetToRead(sparkSession: SparkSession, path: String): Dataset[Client] = {

    import sparkSession.implicits._
    val ds: Dataset[Client] = sparkSession
      .read
      .option("header", true)
      .option("delimiter", ";")
      .csv(path).coalesce(1)
      .withColumn("IdentifiantClient", 'IdentifiantClient.cast(LongType))
      .as[Client]

    ds
  }

  def dataSetToWrite(path: String, ds: Dataset[Client]): Unit = {
    ds.write
      .option("header", true)
      .option("delimiter", ";")
      .mode("overwrite")
      .csv(path)
  }

  def deleteById(sparkSession: SparkSession): Unit = {
    val sparkSession = SparkSession.builder().appName("dataproject-Amal-Kinan").master("local").getOrCreate()


    val bPath = "data"
    val dataset = dataSetToRead(sparkSession, bPath)
    dataset.show()
    dataset.printSchema()
    val clean = delete(dataset, 2564881)
    clean.show()
    dataSetToWrite(bPath + "v2" , clean)
  }



 }