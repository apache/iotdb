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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.nio.ByteBuffer;
import java.util.List;

public interface OperationQuota {

  public enum OperationType {
    READ,
    WRITE,
  }

  /**
   * Checks if it is possible to execute the specified operation. The quota will be estimated based
   * on the number of operations to perform and the average size accumulated during time.
   *
   * @param numWrites number of write operation that will be performed
   * @param numReads number of small-read operation that will be performed
   * @throws RpcThrottlingException if the operation cannot be performed because RPC quota is
   *     exceeded.
   */
  void checkQuota(int numWrites, int numReads, Statement s) throws RpcThrottlingException;

  void addReadResult(List<ByteBuffer> queryResult);

  /** Cleanup method on operation completion */
  void close();
}
