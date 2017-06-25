package proyectoDF.cluster.nodo


import java.net.NetworkInterface

import language.postfixOps
import scala.concurrent.Future
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.pipe
import akka.pattern.ask
import jdk.nashorn.internal.runtime.regexp.joni.Config
import proyectoDF.cluster.mensajeria.{peticionDF, respuestaDF}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.HashMap

class nodoDataFederation extends Actor {

  var contador = 0

//  val conf = new SparkConf().setAppName("SparkNodoConexion").setMaster("spark://utad-virtual-machine:7077")//.set("spark.ui.port", "44040" )
//  val sc = new SparkContext(conf)


  val spark = org.apache.spark.sql.SparkSession.builder
    .master("spark://df-machine-01:7077")
    .appName("SparkNodoConexion")
      .config("spark.cores.max", 1)
    .getOrCreate;

  val df = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load("/home/utad/Descargas/airports.csv"); //.csv("csv/file/path") //spark 2.0 api

  // after registering as a table you will be able to run sql queries
  df.registerTempTable("airports")



  // create Spark context with Spark configuration
  def receive = {
    case job: peticionDF =>
      println(s"Recibida peticion en DataFederation (): '${job.texto}'")
      contador += 1

      // Load our input data.
      val resultado = spark.sql(s"select airport from airports where iata like '${job.texto}'")

      resultado.collect().foreach(t => sender() ! respuestaDF("Respuesta: "+t(0)))
//      implicit val timeout = Timeout(1 seconds)

      sender() ! respuestaDF(s"Fin de la Respuesta del DataFederation a: '${job.texto}'")

  }
}
