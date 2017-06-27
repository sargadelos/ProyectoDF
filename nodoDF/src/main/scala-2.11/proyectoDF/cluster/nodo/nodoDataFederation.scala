package proyectoDF.cluster.nodo

import language.postfixOps
import akka.actor.{Actor, ActorPath, ActorRef, ActorSystem, Terminated}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Send, SendToAll, Subscribe}
import proyectoDF.cluster.mensajeria.{metaDataDF, peticionDF, respuestaDF}


class nodoDataFederation extends Actor {

  // Variables para replicacion metada
  val mediator = DistributedPubSub(context.system).mediator
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("spark://192.168.1.20:7077")
    .appName("SparkNodoConexion")
    .config("spark.cores.max", 1)
    .getOrCreate()

  override def preStart() {
    // Abrimos la sesion de Spark

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/utad/Descargas/airports.csv"); //.csv("csv/file/path") //spark 2.0 api

    // after registering as a table you will be able to run sql queries
    df.createOrReplaceTempView("airports")

  }

  def procesarPeticion (textoPeticion: String) : Unit = {
    println(s"Procesamos comando SQL DataFederation (): '$textoPeticion")

    // Load our input data.
    val resultado = spark.sql(s"select airport from airports where iata like '$textoPeticion'")

    resultado.collect().foreach(t => sender() ! respuestaDF("Respuesta: "+t(0)))

  }

  // create Spark context with Spark configuration
  def receive = {
    case job: peticionDF =>
      // Recibimos mensaje de peticion
      println(s"Recibida peticion en DataFederation (): '${job.text}'")
      // procesamos la peticion
      procesarPeticion (job.text)

      // Enviamos el mensaje como metadata al resto de nodos salvo él mismo
      println ("Mandamos Metadata")
      mediator ! SendToAll("/user/nodo", metaDataDF(job.text), allButSelf = true)

    case job: metaDataDF =>
      // Si recimbimos metadatos enviamos la peticion pero ya no se reenvia a su vez como metadata
      println(s"Recibido Metadata: ${job.text}")
      procesarPeticion (job.text)
      // Enviamos Metadata a Zookeeper
      // ...... PENDIENTE .......


  }
}
