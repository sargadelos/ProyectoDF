import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import akka.pattern.{ask}
import akka.util.Timeout

import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

object AplicacionCliente {
  def main(args: Array[String]) {

    val system = ActorSystem("SistemaCliente")
    val actorClienteDataFederation =
      system.actorOf(Props[ActorClienteDataFederation],
        name = "actorClienteDataFederation")

    val counter = new AtomicInteger

    var mensaje = new String

    mensaje = scala.io.StdIn.readLine("\n ¿Petición? \n")

    while (mensaje != "FIN") {
      implicit val timeout = Timeout(10 seconds)
      val future = actorClienteDataFederation ? EnviarPeticion (mensaje)
      try {
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println (result)
      }
      catch {
        case e: TimeoutException => println ("No se ha obtenido respuesta en el tiempo máximo establecido. Se cancela la petición.")
        }
      mensaje = scala.io.StdIn.readLine("\n¿Petición? \n")
    }

    system.terminate()
  }
}




