/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.expr.simulator;

public class SimulationStatistics {
  public long tsFileGeneratedCnt;
  public long modFileGeneratedCnt;

  public long partialDeletionExecutedCnt;
  public long fullDeletionExecutedCnt;
  public long partialDeletionExecutedTime;
  public long fullDeletionExecutedTime;

  public long queryExecutedCnt;
  public long queryReadDeletionTime;
  public long queryReadDeletionSeekTime;
  public long queryReadDeletionTransTime;
  public long queryReadDeletionCnt;
  public long queryReadModsCnt;
  public long queryExecutedTime;

  public long totalDeletionWriteBytes;
  public long totalDeletionReadBytes;

  @Override
  public String toString() {
    return "SimulationStatistics{"
        + "tsFileGeneratedCnt="
        + tsFileGeneratedCnt
        + "\n, modFileGeneratedCnt="
        + modFileGeneratedCnt
        + "\n, partialDeletionExecutedCnt="
        + partialDeletionExecutedCnt
        + "\n, fullDeletionExecutedCnt="
        + fullDeletionExecutedCnt
        + "\n, partialDeletionExecutedTime="
        + partialDeletionExecutedTime
        + "\n, fullDeletionExecutedTime="
        + fullDeletionExecutedTime
        + "\n, queryExecutedCnt="
        + queryExecutedCnt
        + "\n, queryReadDeletionTime="
        + queryReadDeletionTime
        + "\n, queryReadDeletionSeekTime="
        + queryReadDeletionSeekTime
        + "\n, queryReadDeletionTransTime="
        + queryReadDeletionTransTime
        + "\n, queryReadDeletionCnt="
        + queryReadDeletionCnt
        + "\n, queryReadModsCnt="
        + queryReadModsCnt
        + "\n, queryExecutedTime="
        + queryExecutedTime
        + "\n, totalDeletionWriteBytes="
        + totalDeletionWriteBytes
        + "\n, totalDeletionReadBytes="
        + totalDeletionReadBytes
        + '}';
  }
}
