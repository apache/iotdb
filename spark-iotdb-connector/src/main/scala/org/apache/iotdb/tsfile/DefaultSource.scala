package org.apache.iotdb.tsfile

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

private[iotdb] class DefaultSource extends RelationProvider with DataSourceRegister {
  private final val logger = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "tsfile"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    val iotdbOptions = new IoTDBOptions(parameters)

    if (iotdbOptions.url == null || iotdbOptions.sql == null) {
      sys.error("TSFile node or sql not specified")
    }
    new IoTDBRelation(iotdbOptions)(sqlContext.sparkSession)

  }
}