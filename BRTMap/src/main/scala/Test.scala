package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{LogManager, Level, Logger}
import java.sql.Timestamp

object Test {
  def main(args: Array[String]) {
    
  // http://webapibrt.rio.rj.gov.br/api/v1/brt

  val spark = SparkSession.builder().appName("Teste")//.config("spark.master", "local[*]")
      .master("spark://127.0.0.1:7077").getOrCreate()
  
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
        
    
  val veiculos = spark.read.schema(veiculosType).json("hdfs://localhost:9000/user/jonny/data/brt_20170817192501.json")
  
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
  
//  a.printSchema()
//  root
// |-- veiculo: struct (nullable = true)
// |    |-- codigo: string (nullable = true)
// |    |-- datahora: long (nullable = true)
// |    |-- latitude: double (nullable = true)
// |    |-- linha: string (nullable = true)
// |    |-- longitude: double (nullable = true)
// |    |-- velocidade: double (nullable = true)
      
  val b = a
  .withColumn("codigo", (a("veiculo.codigo")))
  .withColumn("datahora", from_unixtime(a("veiculo.datahora")/1000L))
  .withColumn("linha", (a("veiculo.linha")))
  .withColumn("latitude", (a("veiculo.latitude")))
  .withColumn("longitude", (a("veiculo.longitude")))
  .withColumn("velocidade", (a("veiculo.velocidade")))
  .withColumn("id_migracao", (a("veiculo.id_migracao")))
  .withColumn("sentido", (a("veiculo.sentido")))
  .withColumn("trajeto", (a("veiculo.trajeto")))
  .drop(a("veiculo"))
//  
  b.printSchema()
//  b.show(10)
  
  }
  
}