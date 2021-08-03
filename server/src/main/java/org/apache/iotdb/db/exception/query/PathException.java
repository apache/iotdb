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

import org.apache.iotdb.rpc.TSStatusCode;

public class PathException extends QueryProcessException {

  private static final long serialVersionUID = 2141197032898163234L;

  public PathException() {
    super("Timeseries is null", TSStatusCode.PATH_ERROR.getStatusCode());
  }

  public PathException(String message) {
    super(message, TSStatusCode.PATH_ERROR.getStatusCode());
  }

  public PathException(String message, int errorCode) {
    super(message, errorCode);
  }
}
