package proyectoDF.cluster.nodo


import java.nio.charset.Charset
import java.util

import language.postfixOps
import akka.actor.{Actor, FSM}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberEvent, MemberExited, MemberRemoved, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.SendToAll
import com.typesafe.config.ConfigFactory
import org.apache.zookeeper.CreateMode
import proyectoDF.cluster.mensajeria.{inicializadoDF, metaDataDF, peticionDF}
import org.slf4j.LoggerFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.parsing.json.JSONObject
import scala.collection.JavaConversions._


// FSM: Estados
sealed trait EstadoNodoDF
case object Inicializando extends EstadoNodoDF
case object Activo extends EstadoNodoDF

// FSM: Datos
sealed trait Datos
case object SinDatos extends Datos

class nodoDataFederation extends Actor with FSM[EstadoNodoDF, Datos]  {

  val cluster = Cluster(context.system)

  override def preStart(): Unit =  {
    cluster.subscribe(self, classOf[MemberEvent])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  var comandoSQL : String = ""
  var tablaSQL : String = ""

  def parsearSQL(sentencia: String) = {
    // Parseamos la peticion
    val patronComando = "( )*([A-Z]+)( )+(.)*".r
    val patronCreate = "( )*(CREATE|DROP)( )*(TABLE)( )+([A-Z]+)(.)*".r
    val patronSelect = "( )*(SELECT)( )+(.)+( )+(FROM)( )+([A-Z]+)( )*(.)*".r

    try {
      val patronComando(b1, comando: String, b2, b3) = sentencia.toUpperCase
      comandoSQL = comando

      if (comando == "CREATE" || comando == "DROP") {
        val patronCreate(b1, create, b2, t1, b3, tabla, b4) = sentencia.toUpperCase
        tablaSQL = tabla
      }
      else {
        if (comando == "SELECT") {
          val patronSelect(d1, d2, d3, d4, d5, d6, d7, tabla, d9, d10) = sentencia.toUpperCase
          tablaSQL = tabla
        }
        else {
          comandoSQL = "ERROR"
        }
      }
    }
    catch {
      case e: Exception =>
        comandoSQL = "ERROR"
    }
  }


  // Variables para replicacion metadata
  val mediator = DistributedPubSub(context.system).mediator

  startWith(Inicializando, SinDatos)

  val config = ConfigFactory.load()
  val masterSpark = config.getString("sparkConfig.master-url")
  val sparkDriverHost = config.getString("sparkConfig.driver-host")
  val zKHosts = config.getString("zkConfig.hosts")

  // Conexion a Spark
  var spark = org.apache.spark.sql.SparkSession.builder
    .master(masterSpark)
    .appName("SparkNodoConexion")
    .config("spark.cores.max", "2")
    .config("spark.driver.host", sparkDriverHost)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Conexion a Zookeeper utilizando libreria Cliente
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val curatorZookeeperClient = CuratorFrameworkFactory.newClient(zKHosts, retryPolicy)
  curatorZookeeperClient.start
  curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut

  val zkPathMetadata = "/METADATA"
  if (curatorZookeeperClient.checkExists().forPath(zkPathMetadata) == null)
    curatorZookeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPathMetadata)

  // Recuperar metadata existente y aplicarlo
  // Anteriormente hemos chequeado la existencia del nodo metadata y lo hemos creado de no ser asi
  // 1. Leemos la lista de nodos para metadata de tablas
  println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Procesando Metadata en arranque de nodo.")

  var listaTablasZK : java.util.List[String] = curatorZookeeperClient.getChildren().forPath(zkPathMetadata)
  listaTablasZK.foreach( tabla => {
      var listaComandosZK : util.List[String] = curatorZookeeperClient.getChildren().forPath(zkPathMetadata+"/"+tabla)
    listaComandosZK.foreach(comandoZK => {
        val bytesComandoZK = curatorZookeeperClient.getData().forPath(zkPathMetadata+"/"+tabla+"/"+comandoZK)
        val textoComandoZK = new String (bytesComandoZK, Charset.forName("UTF-8"))
        procesarPeticion(textoComandoZK)
    })
    }
  )
  // Cuando ya hemos terminado la inicializacion del nodo lo ponemos en estado Activo para empezar a aceptar peticiones
  self ! inicializadoDF()

  def procesarPeticion (textoPeticion: String) : String = {

    def convertRowToJSON(row: Row): String = {
      try {
        val m = row.getValuesMap(row.schema.fieldNames)
        JSONObject(m).toString()
      }
      catch {
        case e : Exception => {
          println(e.getMessage)
          row.toString()
        }
      }
    }
    var salida: String = ""
    var resultado : DataFrame = null


    println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Procesando comando SQL: '$textoPeticion")

    // Ejecutar comando
    try {
      resultado = spark.sql(textoPeticion)
      if (resultado.rdd.isEmpty() == false) {
        val numFilas = resultado.count
        resultado.rdd.collect().foreach(
            fila => salida = salida + convertRowToJSON(fila) + "\n")
        salida = salida + s"\n  Se han recuperado $numFilas filas.\n"
      }
      else {
        comandoSQL match  {
          case "CREATE" => salida = Console.YELLOW + "[DATAFEDERATION][INFO]:" + Console.WHITE + s" Tabla $tablaSQL creada correctamente"
          case "DROP" => salida = Console.YELLOW + "[DATAFEDERATION][INFO]:" + Console.WHITE + s" Tabla $tablaSQL borrada correctamente"
          case "SELECT" => salida = Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" No se han recuperado filas"
        }
      }
    }
    catch {
      case e: Exception => {
        salida = e.getMessage
      }
    }
    salida
  }

  when (Activo) {
    case Event(peticionDF(texto, est, msj), SinDatos) =>

      // Recibimos mensaje de peticion
      println(Console.YELLOW + "[DATAFEDERATION][INFO]:" + Console.WHITE + s" Recibida peticion en DataFederation (): '$texto'")

      // Parseamos la peticion
      parsearSQL(texto)

      if (comandoSQL == "ERROR")
        sender() ! Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + " SENTENCIA NO VALIDA"
      else
        // procesamos la peticion
        sender() ! procesarPeticion(texto)

      // Enviamos el mensaje como metadata al resto de nodos salvo él mismo
      if (comandoSQL == "CREATE" || comandoSQL == "DROP") {
        println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + " Mandamos Metadata")
        mediator ! SendToAll("/user/nodo", metaDataDF(texto), allButSelf = true)

        // Almacenamos Metadataen ZooKeeper
        val zkPathTabla = zkPathMetadata+"/"+tablaSQL
        val zkPathComando = zkPathMetadata+"/"+tablaSQL+"/"+comandoSQL

        if (curatorZookeeperClient.checkExists().forPath(zkPathTabla) == null)
          curatorZookeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPathTabla)

        if (curatorZookeeperClient.checkExists().forPath(zkPathComando) == null)
          curatorZookeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPathComando)

        curatorZookeeperClient.setData().forPath(zkPathComando, texto.getBytes())

      }

      stay() using SinDatos


    case Event(metaDataDF(texto), SinDatos) =>
      // Si recibimos metadatos enviamos la peticion pero ya no se reenvia a su vez como metadata
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Recibido Metadata: '$texto'")
      procesarPeticion(texto)

      stay() using SinDatos

    case Event (MemberUp(member), SinDatos) =>
      println (Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha conectado un nuevo nodo al cluster (${member.address})")
      stay() using SinDatos

    case Event (MemberRemoved(member, _), SinDatos) =>
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha desconectado un nodo del cluster (${member.address})")
      stay() using SinDatos

    case Event (MemberExited(member), SinDatos) =>
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha desconectado un nodo del cluster (${member.address})")
      stay() using SinDatos
  }

  when (Inicializando) {
    case Event(peticionDF(texto, estado, mensaje), SinDatos) =>
      // Recibida Peticion cuando aun no hay conexion a Spark
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + " Recibida peticion cuando aun no está inicializado el nodo")

      var salida : String = null
      salida = Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + " El servicio del DataFederation aun no está activo. Espere unos segundos"
      sender ! salida

      stay() using SinDatos

    case Event(inicializadoDF(), SinDatos) =>
      // Recibida Peticion cuando aun no hay conexion a Spark
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Nodo Inicializado")

      goto (Activo) using SinDatos

    case Event (MemberUp(member), SinDatos) =>
      println (Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha conectado un nuevo nodo al cluster (${member.address})")
      stay() using SinDatos

    case Event (MemberRemoved(member, _), SinDatos) =>
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha desconectado un nodo del cluster (${member.address})")
      stay() using SinDatos

    case Event (MemberExited(member), SinDatos) =>
      println(Console.YELLOW + s"[DATAFEDERATION][INFO]:" + Console.WHITE + s" Se ha desconectado un nodo del cluster (${member.address})")
      stay() using SinDatos

  }
}