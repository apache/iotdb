/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.spark.db

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}


//IoTDB data partition
case class IoTDBPartition(where: String, id: Int, start: java.lang.Long, end: java.lang.Long) extends Partition {
  override def index: Int = id
}

object IoTDBRDD {

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

}

class IoTDBRDD private[iotdb](
                               sc: SparkContext,
                               options: IoTDBOptions,
                               schema: StructType,
                               requiredColumns: Array[String],
                               filters: Array[Filter],
                               partitions: Array[Partition])
  extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    var finished = false
    var gotNext = false
    var nextValue: Row = _
    val inputMetrics = context.taskMetrics().inputMetrics

    val part = split.asInstanceOf[IoTDBPartition]

    var taskInfo: String = _
    Option(TaskContext.get()).foreach { taskContext =>
      taskContext.addTaskCompletionListener { _ => conn.close() }
      taskInfo = "task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId()
    }

    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
    val conn: Connection = DriverManager.getConnection(options.url, options.user, options.password)
    val stmt: Statement = conn.createStatement()

    var sql = options.sql
    // for different partition
    if (part.where != null) {
      val sqlPart = options.sql.split(SQLConstant.WHERE)
      sql = sqlPart(0) + " " + SQLConstant.WHERE + " (" + part.where + ") "
      if (sqlPart.length == 2) {
        sql += "and (" + sqlPart(1) + ")"
      }
    }
    //
    var rs: ResultSet = stmt.executeQuery(sql)
    val prunedSchema = IoTDBRDD.pruneSchema(schema, requiredColumns)
    private val rowBuffer = Array.fill[Any](prunedSchema.length)(null)

    def getNext: Row = {
      if (rs.next()) {
        val fields = new scala.collection.mutable.HashMap[String, String]()
        for (i <- 1 until rs.getMetaData.getColumnCount + 1) {
          // start from 1
          val field = rs.getString(i)
          fields.put(rs.getMetaData.getColumnName(i), field)
        }

        //index in one required row
        var index = 0
        prunedSchema.foreach((field: StructField) => {
          val r = Converter.toSqlData(field, fields.getOrElse(field.name, null))
          rowBuffer(index) = r
          index += 1
        })
        Row.fromSeq(rowBuffer)

      }
      else {
        finished = true
        null
      }
    }


    override def hasNext: Boolean = {
      if (!finished && !gotNext) {
        nextValue = getNext
        gotNext = true
      }
      !finished
    }

    override def next(): Row = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }

  override def getPartitions: Array[Partition] = partitions


}