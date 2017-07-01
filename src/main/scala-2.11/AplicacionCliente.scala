import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Props, ActorSystem}
import akka.util.Timeout
import scala.io.StdIn
import scala.concurrent.duration._


object AplicacionCliente {
  def main(args: Array[String]) {

    val system = ActorSystem("SistemaCliente")
    val actorClienteDataFederation =
      system.actorOf(Props[ActorClienteDataFederation],
        name = "actorClienteDataFederation")

    val counter = new AtomicInteger

    var mensaje = new String

    mensaje = scala.io.StdIn.readLine("Mensaje? ")

    while (mensaje != "FIN") {

      actorClienteDataFederation ! EnviarPeticion (mensaje)
      mensaje = scala.io.StdIn.readLine("Mensaje? ")
    }

    system.terminate()
  }
}




