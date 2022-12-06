import Serv.Delete.deleteById
import org.apache.spark.sql.{Dataset, SparkSession}
import Serv.Hash.hashClient

import scala.sys.exit


object Main {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder()
      .appName("dataproject-Amal-Kinan")
      .master("local")
      .getOrCreate()

    if ( args(0) == "-delete") {
      print("je suis dans delete pour l'id " + args(0))
      deleteById(sparkSession, args(1).toLong )
    }
    else if (args(0) == "-hash" ) {
      print("je suis dans hash pour l'id " + args(0) )
      hashClient(sparkSession, args(1).toLong )
    }
    else if ((args(0) == "-delete") && (args(2) == "-hash")) {
      print("je suis dans hash et delete")
      deleteById(sparkSession, args(1).toLong )
      hashClient(sparkSession, args(3).toLong )
    }
    else {
      println ("to delete informations about someone :               -delete <id_to_delete>")
      println ("to hash informations about someone :                 -hash <id_to_hash>")
      println ("to delete and hash informations about someone :      -delete <id_to_delete> -hash <id_to_hash>")
      exit(1)
    }

  }

 }