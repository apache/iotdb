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
 *
 */

package org.apache.iotdb.db.exception.query;

import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Date;

public class OutOfTTLException extends WriteProcessException {

  private static final long serialVersionUID = -1197147887094603300L;

  public OutOfTTLException(long insertionTime, long timeLowerBound) {
    super(
        String.format(
            "Insertion time [%s] is less than ttl time bound [%s]",
            new Date(insertionTime), new Date(timeLowerBound)),
        TSStatusCode.OUT_OF_TTL_ERROR.getStatusCode(),
        true);
  }
}
