
1. Ejecutar proyectoDF.cluster.nodo.nodoDataFederationApp con argumento 2551
2. Ejecutar proyectoDF.cluster.nodo.nodoDataFederationApp con argumento 2552 (opcional)
3. Ejecutar AplicacionCliente


Debe estar arrancado Spark. Es recomendable lanzar dos workers.
Debe arrancarse HDFS si los ficheros a partir de los cuales se van crear las tablas van a almacenarse en HDFS
Debe arrancarse Zookeper (basta con un nodo)

Los Datos de conexion a Zookeeper y Spark estan en el archivo application.conf del nodo

Se hace un pequeño parseo de las sentencias. De momento solo se soportan CREATE TABLE, DROP TABLE y SELECT
Cualquier otra sentencia, aunque pueda ser ejecutada en SparkSQL es rechazada por la funcion de parseo.
De momento el cliente solo recibe como respuesta un JSon con el resultado de la primera fila, aunque la select devuelva mas.

Se almacenan como metadatos en Zookeeper las sentencias DDL (CREATE y DROP)
La estructura de nodos es la siguiente:

/METADATA
   |
   |---------> /<NOMBRE_TABLA>
   |               |
   |               |-----------> /<COMANDO (CREATE | DROP)> :: Datos = <Sentencia SQL>
   |               `-----------> /<COMANDO (CREATE | DROP)> :: Datos = <Sentencia SQL>
   |
   `---------> /<NOMBRE_TABLA>
                   |
                   |-----------> /<COMANDO (CREATE | DROP)> :: Datos = <Sentencia SQL>
                   `-----------> /<COMANDO (CREATE | DROP)> :: Datos = <Sentencia SQL>



En el arranque de un nodo lee los metadatos almacenados en Zookeeper y los ejecuta.

Depurados errores.



