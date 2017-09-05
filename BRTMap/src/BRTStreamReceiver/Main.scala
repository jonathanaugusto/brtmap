package BRTStreamReceiver

import com.mysql.jdbc.Driver
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{ LogManager, Level, Logger }
import org.apache.commons.logging.LogFactory
import java.sql.{ Connection, Statement, Timestamp, DriverManager }
import org.apache.spark.sql.streaming.Trigger
import java.sql.Date
import java.util.Calendar
import Array._

// Colocar no crontab:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt -o /usr/local/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json
// HDFS:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt | /usr/local/hadoop/bin/hdfs dfs -put - /user/ubuntu/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json

// Pra rodar no spark-submit:
// bin/spark-submit --master spark://<host>:7077 --jars ../BRTStreamReceiver/mysql-connector-java-5.1.43-bin.jar --num-executors 3 --driver-memory 2g --executor-memory 1g --executor-cores 1 --class BRTStreamReceiver.Main main.scala.BRTStreamReceiver ../BRTStreamReceiver/BRTStreamReceiver.jar <args>

object Main {

//  val veiculoType = StructType(
//    Array(
//      StructField("codigo", StringType),
//      StructField("linha", StringType),
//      StructField("latitude", DoubleType),
//      StructField("longitude", DoubleType),
//      StructField("datahora", DoubleType),
//      StructField("velocidade", DoubleType),
//      StructField("id_migracao", DoubleType),
//      StructField("sentido", StringType),
//      StructField("trajeto", StringType)))
//
//  val veiculosType = StructType(
//    Array(StructField("veiculos", ArrayType(veiculoType))))
  
  val veiculoType = new StructType().add("codigo", StringType).add("linha", StringType).add("latitude", DoubleType).add("longitude", DoubleType).add("datahora", LongType).add("velocidade", DoubleType).add("id_migracao_trajeto", LongType).add("sentido", StringType).add("trajeto", StringType)
      
  val veiculosType = new StructType().add("veiculos", ArrayType(veiculoType))

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: BRTStreamReceiver <files_dir> <mysql_conn> <mysql_usr> <mysql_pwd>")
      System.exit(1)
    }

    val files_dir = args(0)
    val mysql_con = args(1)
    val mysql_usr = args(2)
    val mysql_pwd = args(3)

    val writeToDB = new ForeachWriter[Row] {
      val driver = "com.mysql.jdbc.Driver"
      var connection: Connection = _
      var statement: Statement = _

      def open(partitionId: Long, version: Long): Boolean = {
        // open connection
        Class.forName(driver)
        connection = DriverManager.getConnection(mysql_con, mysql_usr, mysql_pwd)
        statement = connection.createStatement
        true
      }

      def process(record: Row) = {
        // write string to connection
        val columnNames = record.schema.fieldNames.toList
        var command = "INSERT INTO "
        if (columnNames contains "window") command += "agg_vel (" else command += "gpsdata ("
        command += columnNames.mkString(",")
        command += ") VALUES (\""
        command += record.mkString("\",\"")
        command += "\");"

        statement.execute(command)

      }

      def close(errorOrNull: Throwable): Unit = {
        // close the connection
        connection.close
      }
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger().setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
                          .setAppName("PrincipalContext")
                          .setMaster("local[*]")
                          .set("spark.executor.memory","1g")
                          
    val sc = new SparkContext(sparkConf)
    
    val spark = SparkSession
      .builder
      .appName("BRTStreamReceiver")
      .getOrCreate()

    import spark.implicits._

    val veiculos = spark.readStream
      .schema(veiculosType)
      .option("maxFilesPerTrigger",1000)
      .json(files_dir)

    val a = veiculos.select(explode($"veiculos").as("veiculo"))

    val b = a
      .withColumn("codigo", ($"veiculo.codigo"))
      .withColumn("datahora", to_timestamp(from_unixtime($"veiculo.datahora" / 1000L)))
      .withColumn("codlinha", ($"veiculo.linha"))
      .withColumn("latitude", ($"veiculo.latitude"))
      .withColumn("longitude", ($"veiculo.longitude"))
      .withColumn("velocidade", ($"veiculo.velocidade"))
      .withColumn("sentido", ($"veiculo.sentido"))
      .withColumn("nome", ($"veiculo.trajeto"))
      .drop($"veiculo")

    val c = b
      .filter(!($"nome".isNull) && !($"codlinha".isNull))

    val d = c
      .filter(($"codlinha".like("5_____") || $"codlinha".like("__A") || $"codlinha".like("__")))

    val e = d
      .withColumn("linha", trim(split($"nome", "-")(0)))
      .withColumnRenamed("nome", "trajeto")
      .drop($"codlinha")

    val f = e
      .filter($"linha".like("___") || $"linha".like("__"))

    val g = f
      .withColumn("corredor",
        when($"linha".like("1%") or $"linha".like("2%"), "TransOeste").otherwise(
          when($"linha".like("3%") or $"linha".like("4%"), "TransCarioca").otherwise(
            when($"linha".like("5%"), "TransOlímpica").otherwise(""))))

    val h = g.withWatermark("datahora", "10 minutes")
      .dropDuplicates("codigo", "datahora")

    val query = h.writeStream
      .outputMode("update")
      .foreach(writeToDB)
//      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("checkpointLocation", "/home/claudio/workspace_scala/brtmap/BRTMap/resources_1/scripts/checkpoint")
      .start()

    val i = h//.filter($"velocidade" > 0)

    val j = i.groupBy(window($"datahora", "1 minute"), $"linha", $"sentido").avg("velocidade")
      .withColumn("vel_media", $"avg(velocidade)")
      .withColumn("datahora", current_timestamp())
      .drop($"avg(velocidade)")
      .filter($"vel_media" > 0)

    val query2 = i.writeStream
      .outputMode("update")
      .foreach(writeToDB)
//      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("checkpointLocation", "/home/claudio/workspace_scala/brtmap/BRTMap/resources_1/scripts/checkpoint1")
      .start()

    val k = i.groupBy(window($"datahora", "1 hour"), $"linha", $"sentido").avg("velocidade")
      .withColumn("vel_media", $"avg(velocidade)")
      .withColumn("datahora", current_timestamp())
      .drop($"avg(velocidade)")
      .filter($"vel_media" > 0)

    val query3 = k.writeStream
      .outputMode("update")
      .foreach(writeToDB)
      .trigger(Trigger.ProcessingTime("1 hour"))
//      .option("checkpointLocation", "hdfs://192.168.21.2:9000/user/ubuntu/checkpoint3")
      .start()

    val l = i.groupBy(window($"datahora", "1 day"), $"linha", $"sentido").avg("velocidade")
      .withColumn("vel_media", $"avg(velocidade)")
      .withColumn("datahora", current_timestamp())
      .drop($"avg(velocidade)")
      .filter($"vel_media" > 0)

    val query4 = l.writeStream
      .outputMode("update")
      .foreach(writeToDB)
      .trigger(Trigger.ProcessingTime("1 day"))
//      .option("checkpointLocation", "hdfs://192.168.21.2:9000/user/ubuntu/checkpoint4")
      .start()

    query.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
  }
}