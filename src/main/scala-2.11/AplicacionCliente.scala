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
    import system.dispatcher
    system.scheduler.schedule(2.seconds, 2.seconds) {
      actorClienteDataFederation ! EnviarPeticion (counter.incrementAndGet().toString)
      Thread.sleep(1000)
    }

    StdIn.readLine()
    system.terminate()
  }
}




