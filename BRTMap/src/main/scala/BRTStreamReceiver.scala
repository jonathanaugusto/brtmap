package main.scala

import com.mysql.jdbc.Driver
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{ LogManager, Level, Logger }
import org.apache.commons.logging.LogFactory
import java.sql.{ Connection, Statement, Timestamp, DriverManager }
import org.apache.spark.sql.streaming.ProcessingTime

// Colocar no crontab:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt -o /usr/local/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json
// HDFS:
// * * * * *  curl -N http://webapibrt.rio.rj.gov.br/api/v1/brt | /usr/local/hadoop/bin/hdfs dfs -put - /user/ubuntu/data/brt_$(date +\%Y\%m\%d\%H\%M\%S).json

// Pra rodar no spark-submit:
// bin/spark-submit --deploy-mode cluster --master spark://localhost:7077 --jars ../BRTStreamReceiver/mysql-connector-java-5.1.43-bin.jar --class main.scala.BRTStreamReceiver ../BRTStreamReceiver/BRTStreamReceiver.jar <args>

object BRTStreamReceiver {

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
      System.err.println("Usage: BRTStreamReceiver <files_dir> <mysql_url> <mysql_user> <mysql_pwd> <mysql_schema> <mysql_table>")
      System.exit(1)
    }

    val directory = args(0)
    val mysql_url = args(1)
    val mysql_usr = args(2)
    val mysql_pwd = args(3)
    val mysql_sch = args(4)
    val mysql_tab = args(5)

    val writeToDB = new ForeachWriter[Row] {
      val driver = "com.mysql.jdbc.Driver"
      var connection: Connection = _
      var statement: Statement = _

      def open(partitionId: Long, version: Long): Boolean = {
        // open connection
        Class.forName(driver)
        connection = DriverManager.getConnection("jdbc:mysql://"+ mysql_url + "/" + mysql_sch, mysql_usr, mysql_pwd)
        statement = connection.createStatement
        true
      }

      def process(record: Row) = {
        // write string to connection
        statement.executeUpdate("INSERT INTO " +
          "gpsdata (codigo, linha, latitude, longitude, datahora, velocidade, id_migracao, sentido, trajeto) VALUES ('" +
          record.getAs[String]("codigo") + "','" + record.getAs[String]("linha") + "'," + record.getAs[Double]("latitude") + "," +
          record.getAs[Double]("longitude") + ",'" + record.getAs[Timestamp]("datahora") + "'," + record.getAs[Double]("velocidade") + ",'" +
          record.getAs[String]("id_migracao") + "','" + record.getAs[String]("sentido") + "',\"" + record.getAs[String]("trajeto") + "\")")
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
      .json(directory)

    val a = veiculos.select(explode($"veiculos").as("veiculo"))

    val b = a
      .withColumn("codigo", (a("veiculo.codigo")))
      .withColumn("linha", (a("veiculo.linha")))
      .withColumn("latitude", (a("veiculo.latitude")))
      .withColumn("longitude", (a("veiculo.longitude")))
      .withColumn("datahora", to_timestamp(from_unixtime(a("veiculo.datahora") / 1000L)))
      .withColumn("velocidade", (a("veiculo.velocidade")))
      .withColumn("id_migracao", (a("veiculo.id_migracao")))
      .withColumn("sentido", (a("veiculo.sentido")))
      .withColumn("trajeto", (a("veiculo.trajeto")))
      .drop(a("veiculo"))

    //root
    // |-- codigo: string (nullable = true)
    // |-- linha: string (nullable = true)
    // |-- latitude: double (nullable = true)
    // |-- longitude: double (nullable = true)
    // |-- datahora: timestamp (nullable = true)
    // |-- velocidade: double (nullable = true)
    // |-- id_migracao: double (nullable = true)
    // |-- sentido: string (nullable = true)
    // |-- trajeto: string (nullable = true)

    val c = b.withWatermark("datahora", "10 minutes")
      .dropDuplicates("codigo", "datahora")

    val query = c.writeStream
      .outputMode("update")
      .foreach(writeToDB)
      //.format("console")
      .trigger(ProcessingTime("1 minute"))
      .start()

    query.awaitTermination()
  }
}
