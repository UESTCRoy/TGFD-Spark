package tgfdDiscovery.script.dbpedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CustomDBPediaGraph {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Generate Custom DBPedia Graph")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val edgeSize = args(2)


  }
}
