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

package org.apache.iotdb.udf.api.relational.table;

import org.apache.iotdb.udf.api.i18n.UdfApiMessages;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;

public interface TableFunctionProcessorProvider {
  /**
   * This method returns a {@linkplain TableFunctionDataProcessor}. All the necessary information
   * collected during analysis is available in the implementation of TableFunctionProcessorProvider.
   * It is called once per each partition processed by the table function.
   */
  default TableFunctionDataProcessor getDataProcessor() {
    throw new UnsupportedOperationException(
        UdfApiMessages.TABLE_FUNCTION_DOES_NOT_PROCESS_INPUT_DATA);
  }

  /**
   * This method returns a {@linkplain TableFunctionLeafProcessor}. All the necessary information
   * collected during analysis is available in the implementation of TableFunctionProcessorProvider.
   * It is called once per each split processed by the table function.
   */
  default TableFunctionLeafProcessor getSplitProcessor() {
    throw new UnsupportedOperationException(
        UdfApiMessages.TABLE_FUNCTION_DOES_NOT_PROCESS_LEAF_DATA);
  }

  /**
   * The bytes the processors of this provider may hold in their own buffers while a partition is
   * being processed, above what {@code process} returns. The engine adds this to the estimated
   * memory of the {@linkplain TableFunctionDataProcessor} operator, so a table function that
   * accumulates input (rather than emitting a result per record) is visible to memory management
   * and can be back-pressured. The default is {@code 0} for table functions that keep no state
   * across calls.
   */
  default long getEstimatedMaxBufferedBytes() {
    return 0L;
  }
}
