package lib

import Configuration.Client
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SparkSession}

object ReadAndWrite {

  def dataSetToRead(sparkSession: SparkSession, path: String): Dataset[Client] = {

    import sparkSession.implicits._
    val data: Dataset[Client] = sparkSession
      .read
      .option("header", true)
      .option("delimiter", ";")
      .csv(path).coalesce(1)
      .withColumn("IdentifiantClient", 'IdentifiantClient.cast(LongType))
      .as[Client]

    data
  }

  def dataSetToWrite(path: String, ds: Dataset[Client]): Unit = {
    ds.write
      .option("header", true)
      .option("delimiter", ";")
      .mode("overwrite")
      .csv(path)
  }
}
