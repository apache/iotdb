package org.apache.iotdb

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object tsfile {

  /**
    * add a method 'tsfile' to DataFrameReader to read tsfile
    */
  implicit class TsFileDataFrameReader(reader: DataFrameReader) {
    def tsfile: String => DataFrame = reader.format("org.apache.iotdb.tsfile").load
  }

  /**
    * add a method 'tsfile' to DataFrameWriter to write tsfile
    */
  implicit class TsFileDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def tsfile: String => Unit = writer.format("org.apache.iotdb.tsfile").save
  }
}
