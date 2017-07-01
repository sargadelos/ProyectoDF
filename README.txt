
1. Ejecutar proyectoDF.cluster.nodo.nodoDataFederationApp con argumento 2551
2. Ejecutar proyectoDF.cluster.nodo.nodoDataFederationApp con argumento 2552 (opcional)
3. Ejecutar AplicacionCliente


Debe estar arrancado Spark. Es recomendable lanzar dos workers.
Debe arrancarse HDFS si los ficheros a partir de los cual se van crear las tablas van a almacenarse en HDFS
Debe arrancarse Zookeper

Los Datos de conexion a Zookeeper y Spark estan en el archivo application.conf del nodo

Se hace un pequeño parseo de las sentencias. De momento solo se soportan CREATE TABLE, DROP TABLE y SELECT
Cualquier otra sentencia, aunque pueda ser ejecutada en SparkSQL es rechazada por la funcion de parseo.
De momento el cliente solo recibe como respuesta un JSon con el resultado de la primera fila, aunque la select devuelva mas.

Se almacenan como metadatos en Zookeeper las sentencias DDL (CREATE y DROP)

Pendiente de desarrollar la carga inicial de los metadatos en el arranque de un nodo.


