package proyectoDF.cluster.nodo

import language.postfixOps
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.cluster.client.ClusterClientReceptionist


    object nodoDataFederationApp extends App {

  override def main(args: Array[String]): Unit = {

    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [nodo]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterDataFederation", config)
    val nodo = system.actorOf(Props[nodoDataFederation], name = "nodo")
    ClusterClientReceptionist(system).registerService(nodo)
  }

}