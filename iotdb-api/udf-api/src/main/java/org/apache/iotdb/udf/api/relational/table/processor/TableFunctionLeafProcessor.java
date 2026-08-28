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

package org.apache.iotdb.udf.api.relational.table.processor;

import org.apache.iotdb.udf.api.IoTDBLocal;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;

public interface TableFunctionLeafProcessor {

  default void beforeStart() {
    // do nothing
  }

  /** Same as {@link #beforeStart()} with access to {@link IoTDBLocal} for embedded queries. */
  default void beforeStart(IoTDBLocal local) {
    beforeStart();
  }

  /**
   * This method processes a portion of data. It is called multiple times until the processor is
   * fully processed.
   *
   * @param columnBuilders a list of {@link ColumnBuilder} for each column in the output table.
   */
  void process(List<ColumnBuilder> columnBuilders);

  /** Same as {@link #process(List)} with access to {@link IoTDBLocal} for embedded queries. */
  default void process(List<ColumnBuilder> columnBuilders, IoTDBLocal local) {
    process(columnBuilders);
  }

  /** This method is called to determine if the processor has finished processing all data. */
  boolean isFinish();

  /** This method is mainly used to release the resources used in the UDF. */
  default void beforeDestroy() {
    // do nothing
  }

  /** Same as {@link #beforeDestroy()} with access to {@link IoTDBLocal}. */
  default void beforeDestroy(IoTDBLocal local) {
    beforeDestroy();
  }
}
