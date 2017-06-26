package proyectoDF.cluster.nodo




import language.postfixOps
import scala.concurrent.Future
import akka.actor.{Actor, ActorPath, ActorRef, ActorSystem, Terminated}
import akka.util.Timeout
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, SendToAll, Send, Subscribe}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.pipe
import akka.pattern.ask
import jdk.nashorn.internal.runtime.regexp.joni.Config
import proyectoDF.cluster.mensajeria.{metaDataDF, peticionDF, respuestaDF}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import akka.cluster.client.{ClusterClient, ClusterClientSettings}

import scala.collection.mutable.HashMap

class nodoDataFederation extends Actor {

  var contador = 0

  // Variables para replicacion metada
  val mediator = DistributedPubSub(context.system).mediator

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
  df.createOrReplaceTempView("airports")


  // create Spark context with Spark configuration
  def receive = {
    case job: peticionDF =>
      println(s"Recibida peticion en DataFederation (): '${job.text}'")
      contador += 1

      // Load our input data.
      val resultado = spark.sql(s"select airport from airports where iata like '${job.text}'")

      resultado.collect().foreach(t => sender() ! respuestaDF("Respuesta: "+t(0)))
//      implicit val timeout = Timeout(1 seconds)

//      sender() ! respuestaDF(s"Fin de la Respuesta del DataFederation a: '${job.text}'")
      println ("Mandamos Metadata")
      mediator ! SendToAll("/user/nodo", metaDataDF(job.text), allButSelf = true)

    case job: metaDataDF =>
      println(s"Metadata: ${job.text}")
      if (sender == self)
        println("Recibida metadata propio: no hacemos nada")
      else
        println("Recibida metadata ajeno: lo reenviamos como peticion")
        self ! peticionDF (job.text)

  }
}
