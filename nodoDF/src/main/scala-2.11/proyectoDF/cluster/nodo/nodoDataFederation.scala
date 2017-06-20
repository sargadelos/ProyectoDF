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
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
class nodoDataFederation extends Actor {

  var contador = 0

  // create Spark context with Spark configuration
  val conf = new SparkConf().setAppName("SparkNodoConexion").setMaster("local")
  val sc = new SparkContext(conf)

  def receive = {
    case job: peticionDF =>
      println(s"Recibida peticion en DataFederation (): '$job'")
      contador += 1
      implicit val timeout = Timeout(5 seconds)
      sender() ! respuestaDF(s"Respuesta del DataFederation a: '$job'")

  }
}
