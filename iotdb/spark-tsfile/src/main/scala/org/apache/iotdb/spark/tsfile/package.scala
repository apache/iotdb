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

package org.apache.iotdb.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object tsfile {

  val myPackage = "org.apache.iotdb.spark.tsfile"

  /**
    * add a method 'tsfile' to DataFrameReader to read tsfile
    *
    * @param reader dataframeReader
    */
  implicit class TsFileDataFrameReader(reader: DataFrameReader) {
    def tsfile(path: String,
               isNarrowForm: Boolean = false): DataFrame = {
      if (isNarrowForm) {
        reader.option(DefaultSource.path, path).option(DefaultSource.isNarrowForm, "narrow_form").
          format(myPackage).load
      }
      else {
        reader.option(DefaultSource.path, path).format(myPackage).load
      }
    }
  }

  /**
    * add a method 'tsfile' to DataFrameWriter to write tsfile
    */
  implicit class TsFileDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def tsfile(path: String,
               isNarrowForm: Boolean = false): Unit = {
      if (isNarrowForm) {
        writer.option(DefaultSource.path, path).option(DefaultSource.isNarrowForm, "narrow_form").
          format(myPackage).save
      }
      else {
        writer.option(DefaultSource.path, path).format(myPackage).save
      }
    }
  }

}
