package BRTStreamReceiver

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{LogManager, Level, Logger}
import java.sql.Timestamp

object Test {
  def main(args: Array[String]) {
    
  // http://webapibrt.rio.rj.gov.br/api/v1/brt

  val spark = SparkSession.builder().appName("Teste")//.config("spark.master", "local[*]")
      .master("local[1]")
      .getOrCreate()
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  LogManager.getRootLogger().setLevel(Level.ERROR)

  import spark.implicits._
  
//  case class Veiculo (codigo: String, linha: String, latitude: Double, 
//        longitude: Double, datahora: Timestamp, velocidade: Double)
//  case class Veiculos (veiculos: Array[Veiculo])
  
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
    
  val veiculos = spark.read.schema(veiculosType).json("file:///usr/local/data/brt_20170817182501.json")
  
  
  
// veiculos.printSchema()
//  root
// |-- veiculos: array (nullable = true)
// |    |-- element: struct (containsNull = true)
// |    |    |-- codigo: string (nullable = true)
// |    |    |-- datahora: long (nullable = true)
// |    |    |-- latitude: double (nullable = true)
// |    |    |-- linha: string (nullable = true)
// |    |    |-- longitude: double (nullable = true)
// |    |    |-- velocidade: double (nullable = true)
  
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
      
    val pre3 = b
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

//window($"datahora", "1 hour"),
  val k = pre5.groupBy(window($"datahora", "10 minutes"), $"linha", $"sentido")
      .agg(avg("velocidade"), count("codigo"))
      .withColumn("window_start", ($"window.start"))
      .withColumn("window_end", ($"window.end"))
      .drop($"window")
      .withColumnRenamed("avg(velocidade)", "vel_media")
      .withColumnRenamed("count(codigo)", "qtd_carros")
      .withColumn("datahora", current_timestamp())

  
  k.printSchema()
  k.show(200, false)
  
  }
  
}