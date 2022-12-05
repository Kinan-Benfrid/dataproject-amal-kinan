import Configuration.Client
import lib.ReadAndWrite
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import Serv.Hash
import Serv.Hash.hashClient


object Main {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder()
      .appName("dataproject-Amal-Kinan")
      .master("local")
      .getOrCreate()


    //deleteById(sparkSession: SparkSession)
    hashClient(sparkSession, 2564888)

  }

  def delete(dataset: Dataset[Client], id: Long): Dataset[Client] = {
    val clean = dataset.filter(!col("IdentifiantClient").isin(id))
    clean
  }



  def deleteById(sparkSession: SparkSession): Unit = {
    val sparkSession = SparkSession.builder().appName("dataproject-Amal-Kinan").master("local").getOrCreate()


    val bPath = "data"
    val dataset = ReadAndWrite.dataSetToRead(sparkSession, bPath)
    dataset.show()
    dataset.printSchema()
    val clean = delete(dataset, 2564881)
    clean.show()
    ReadAndWrite.dataSetToWrite(bPath + "v2" , clean)
  }



 }