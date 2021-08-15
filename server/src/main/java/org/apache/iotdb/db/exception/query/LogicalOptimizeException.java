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
package org.apache.iotdb.db.exception.query;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.rpc.TSStatusCode;

/** This exception is thrown while meeting error in optimizing logical operator. */
public class LogicalOptimizeException extends LogicalOperatorException {

  private static final long serialVersionUID = -7098092782689670064L;

  public LogicalOptimizeException(String message) {
    super(message, TSStatusCode.LOGICAL_OPTIMIZE_ERROR.getStatusCode());
  }

  public LogicalOptimizeException(String filterOperator, FilterType filterType) {
    super(
        String.format(
            "Unknown token in [%s]: [%s], [%s].",
            filterOperator, filterType, FilterConstant.filterNames.get(filterType)),
        TSStatusCode.LOGICAL_OPTIMIZE_ERROR.getStatusCode());
  }

  public LogicalOptimizeException(IoTDBException e) {
    super(e);
  }
}
