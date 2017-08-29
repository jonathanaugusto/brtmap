package BRTStreamReceiver

import com.mysql.jdbc.Driver
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{ LogManager, Level, Logger }
import org.apache.commons.logging.LogFactory
import java.sql.{ Connection, Statement, Timestamp, DriverManager }
import org.apache.spark.sql.streaming.Trigger
import java.sql.Date
import java.util.Calendar

// Colocar no crontab:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt -o /usr/local/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json
// HDFS:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt | /usr/local/hadoop/bin/hdfs dfs -put - /user/ubuntu/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json

// Pra rodar no spark-submit:
// bin/spark-submit --master spark://<host>:7077 --jars ../BRTStreamReceiver/mysql-connector-java-5.1.43-bin.jar --num-executors 3 --driver-memory 2g --executor-memory 1g --executor-cores 1 --class BRTStreamReceiver.Main main.scala.BRTStreamReceiver ../BRTStreamReceiver/BRTStreamReceiver.jar <args>

object Main {

  val veiculoType = StructType(
    Array(
      StructField("codigo", StringType),
      StructField("linha", StringType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("datahora", DoubleType),
      StructField("velocidade", DoubleType),
      StructField("id_migracao", DoubleType),
      StructField("sentido", StringType),
      StructField("trajeto", StringType)))

  val veiculosType = StructType(
    Array(StructField("veiculos", ArrayType(veiculoType))))

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
        if       (columnNames contains "vel_media")   command += "stats_vel (" 
        else if  (columnNames contains "qtd_carros")  command += "stats_qtd ("
        else                                          command += "gpsdata ("
        command += columnNames.mkString(",")
        command += ") VALUES (\""
        command += record.mkString("\",\"")
        command += "\");"

        statement.executeUpdate(command)

      }

      def close(errorOrNull: Throwable): Unit = {
        // close the connection
        connection.close
      }
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger().setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("BRTStreamReceiver")
      .getOrCreate()

    import spark.implicits._

    val veiculos = spark.readStream
      .schema(veiculosType)
      .option("maxFilesPerTrigger",1000)
      .json(files_dir)

    val pre1 = veiculos.select(explode($"veiculos").as("veiculo"))

    val pre2 = pre1
      .withColumn("codigo", ($"veiculo.codigo"))
      .withColumn("datahora", to_timestamp(from_unixtime($"veiculo.datahora" / 1000L)))
      .withColumn("codlinha", ($"veiculo.linha"))
      .withColumn("latitude", ($"veiculo.latitude"))
      .withColumn("longitude", ($"veiculo.longitude"))
      .withColumn("velocidade", ($"veiculo.velocidade"))
      .withColumn("sentido", ($"veiculo.sentido"))
      .withColumn("nome", ($"veiculo.trajeto"))
      .drop($"veiculo")
      
    val pre3 = pre2
      .filter(!($"nome".isNull) && !($"codlinha".isNull))
      .filter(($"codlinha".like("5_____") || $"codlinha".like("__A") || $"codlinha".like("__")))
      
    val pre4 = pre3
      .withColumn("linha", trim(split($"nome", "-")(0)))
      .filter($"linha".like("___") || $"linha".like("__"))
      .withColumn("corredor",
       when($"linha".like("1%") or $"linha".like("2%"), "TransOeste").otherwise(
       when($"linha".like("3%") or $"linha".like("4%"), "TransCarioca").otherwise(
       when($"linha".like("5%"), "TransOlímpica").otherwise(""))))
            
    val pre5 = pre4
      .withColumnRenamed("nome", "trajeto")
      .drop($"codlinha")

    val realtimedata = pre5
      .withWatermark("datahora", "10 minutes")
      .dropDuplicates("codigo", "datahora")

    // Query 1: dados em tempo real
    val query1 = realtimedata.writeStream
      .outputMode("update")
      .foreach(writeToDB)
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .option("checkpointLocation", "hdfs://192.168.21.2:9000/user/ubuntu/checkpoint")
      .start()

      
    // Query 2: média de velocidade - janela de 1 hora
    val group2 = realtimedata.groupBy(window($"datahora", "1 hour"), $"corredor")
      .agg(avg("velocidade"))
      .withColumn("data", to_date(date_format($"window.start", "yyyy-MM-dd")))
      .withColumn("hora", date_format($"window.start", "HH:mm"))
      .drop($"window")
      .withColumnRenamed("avg(velocidade)", "vel_media")
      .withColumn("atualizacao", current_timestamp())
      
    val query2 = group2.writeStream
      .outputMode("update")
      .foreach(writeToDB)
//      .trigger(Trigger.ProcessingTime("1 hour"))
//      .option("checkpointLocation", "hdfs://192.168.21.2:9000/user/ubuntu/checkpoint3")
      .start()

    // Query 3: quantidade de carros - janela de 1 hora
    val group3 = realtimedata.groupBy(window($"datahora", "1 hour"), $"corredor")
      .agg(approx_count_distinct("codigo"))
      .withColumn("data", to_date(date_format($"window.start", "yyyy-MM-dd")))
      .withColumn("hora", date_format($"window.start", "HH:mm"))
      .drop($"window")
      .withColumnRenamed("count(codigo)", "qtd_carros")
      .withColumn("atualizacao", current_timestamp())
      
    val query3 = group3.writeStream
      .outputMode("update")
      .foreach(writeToDB)
//      .trigger(Trigger.ProcessingTime("1 day"))
//      .option("checkpointLocation", "hdfs://192.168.21.2:9000/user/ubuntu/checkpoint4")
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
  }
}
