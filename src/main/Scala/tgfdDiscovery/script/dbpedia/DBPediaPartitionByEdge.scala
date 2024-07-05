package tgfdDiscovery.script.dbpedia

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.script.GraphPartitionByEdge.{attributeRegex, literalRegex, logger, uriRegex}

import scala.util.matching.Regex

object DBPediaPartitionByEdge {
  val logger = Logger.getLogger(this.getClass.getName)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 3) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("IMDB Graph Partitioner")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val numPartitions = args(2).toInt

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    fileStatusList.foreach { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileName = fileStatus.getPath.getName
      val fileContent = spark.sparkContext.textFile(filePath)
      println(s"Processing file: $filePath")

      // 解析文件内容
      val parsedTriplets = fileContent.flatMap { line =>
        Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
          val uriMatches = uriRegex.findAllIn(trimmedLine).toList
          val literalMatches = literalRegex.findAllIn(trimmedLine).toList

          (uriMatches, literalMatches) match {
            case (List(subject, predicate), List(literal)) =>
              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
              Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
            case (List(subject, predicate, obj), _) =>
              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
              Some((subject, attributeName, obj, "edge"))
            case _ => None
          }
        }
      }


    }
  }
}
