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

package org.apache.iotdb.procedure.service;

import org.apache.iotdb.procedure.Procedure;
import org.apache.iotdb.procedure.ProcedureExecutor;
import org.apache.iotdb.service.rpc.thrift.ProcedureService;
import org.apache.iotdb.service.rpc.thrift.SubmitProcedureReq;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProcedureServerProcessor implements ProcedureService.Iface {
  private ProcedureExecutor executor;

  private static final String PROC_NOT_FOUND = "Proc  %d not found";
  private static final String PROC_ABORT = "Proc %d  is  aborted";
  private static final String PROC_ABORT_FAILED = "Abort  %d  failed";
  private static final String PROC_SUBMIT = "Proc  is submitted";

  public ProcedureServerProcessor() {}

  public ProcedureServerProcessor(ProcedureExecutor executor) {
    this.executor = executor;
  }

  @Override
  public String query(long procId) {
    Procedure procedure = executor.getResultOrProcedure(procId);
    if (null != procedure) {
      return procedure.toString();
    } else {
      return String.format(PROC_NOT_FOUND, procId);
    }
  }

  @Override
  public String abort(long procId) {
    return executor.abort(procId)
        ? String.format(PROC_ABORT, procId)
        : String.format(PROC_ABORT_FAILED, procId);
  }

  @Override
  public long submit(SubmitProcedureReq req) throws TException {
    byte[] procedureBody = req.getProcedureBody();
    long procId;
    ByteBuffer byteBuffer = ByteBuffer.wrap(procedureBody);
    Procedure procedure = Procedure.newInstance(byteBuffer);
    try {
      procedure.deserialize(byteBuffer);
      procId = executor.submitProcedure(procedure);
    } catch (IOException e) {
      return -1;
    }
    return procId;
  }
}
