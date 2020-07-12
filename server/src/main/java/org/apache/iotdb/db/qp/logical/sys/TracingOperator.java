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
<<<<<<< Updated upstream:server/src/main/java/org/apache/iotdb/db/qp/logical/sys/TracingOperator.java
package org.apache.iotdb.db.qp.logical.sys;
=======
package org.apache.iotdb.db.exception.index;
>>>>>>> Stashed changes:server/src/main/java/org/apache/iotdb/db/exception/index/UnSupportedIndexTypeException.java

import org.apache.iotdb.db.qp.logical.RootOperator;

<<<<<<< Updated upstream:server/src/main/java/org/apache/iotdb/db/qp/logical/sys/TracingOperator.java
public class TracingOperator extends RootOperator {

  private boolean isTracingon;

  public TracingOperator(int tokenIntType, boolean isTracingon) {
    super(tokenIntType);
    this.isTracingon = isTracingon;
    operatorType = OperatorType.TRACING;
  }

  public boolean isTracingon() {
    return isTracingon;
=======
public class UnSupportedIndexTypeException extends IndexException {

  private static final long serialVersionUID = 4967425512171623007L;

  public UnSupportedIndexTypeException(String indexType) {
    super(String.format("Unsupported index type: [%s]", indexType),
        TSStatusCode.UNSUPPORTED_INDEX_TYPE_ERROR.getStatusCode());
>>>>>>> Stashed changes:server/src/main/java/org/apache/iotdb/db/exception/index/UnSupportedIndexTypeException.java
  }
}
