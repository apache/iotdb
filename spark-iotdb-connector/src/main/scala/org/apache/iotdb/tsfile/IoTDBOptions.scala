package org.apache.iotdb.tsfile

/**
  * Created by qjl on 16-11-4.
  */
class IoTDBOptions(
                     @transient private val parameters: Map[String, String])
  extends Serializable {

  val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))

  val user = parameters.getOrElse("user", "root")

  val password = parameters.getOrElse("password", "root")

  val sql = parameters.getOrElse("sql", sys.error("Option 'sql' not specified"))

  // deprecated:

  val delta_object = parameters.getOrElse("delta_object", "unsupported")

  val numPartition = parameters.getOrElse("numPartition", "1")

  val lowerBound = parameters.getOrElse("lowerBound", "0")

  val upperBound = parameters.getOrElse("upperBound", "0")

  def get(name: String): Unit = {

  }
}
