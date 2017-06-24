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
  val conf = new SparkConf().setAppName("SparkNodoConexion").setMaster("spark://utad-virtual-machine:7077")
  val sc = new SparkContext(conf)

  def receive = {
    case job: peticionDF =>
      println(s"Recibida peticion en DataFederation (): '$job'")
      contador += 1
      implicit val timeout = Timeout(5 seconds)

      // Load our input data.
      println("Leer Fichero")
      val input =  sc.textFile("/home/utad/Descargas/test.csv")
      // Split up into words.
      println("FlatMap")
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      println("Map")
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      println("Escribir Fichero")
      counts.saveAsTextFile("/home/utad/Descargas/Proyecto/"+contador+".txt")
      sender() ! respuestaDF(s"Respuesta del DataFederation a: '$job'")

  }
}
