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

import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.procedure.conf.ProcedureNodeConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcedureServerCommandLine extends ServerCommandLine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureServerCommandLine.class);

  @Override
  protected String getUsage() {
    return null;
  }

  @Override
  protected int run(String[] args) {
    try {
      StartupChecks checks = new StartupChecks().withDefaultTest();
      checks.verify();
      ProcedureNodeConfigDescriptor.getInstance().checkConfig();
      ProcedureNode procedureNode = ProcedureNode.getInstance();
      procedureNode.active();
    } catch (StartupException e) {
      LOGGER.info("Meet error when doing start  checking", e);
      return -1;
    }
    return 0;
  }
}
