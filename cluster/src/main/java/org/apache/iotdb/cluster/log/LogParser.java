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

package org.apache.iotdb.cluster.log;

import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log.Types;
import org.apache.iotdb.cluster.log.logs.AddNodeLog;
import org.apache.iotdb.cluster.log.logs.PhysicalPlanLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogParser transform a ByteBuffer into a Log.
 */
public class LogParser {

  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
  private static final LogParser INSTANCE = new LogParser();

  private LogParser() {
    // singleton class
  }

  public static LogParser getINSTANCE() {
    return INSTANCE;
  }

  public Log parse(ByteBuffer buffer) throws UnknownLogTypeException {
    if (logger.isDebugEnabled()) {
      logger.debug("Received a log buffer, pos:{}, limit:{}", buffer.position(), buffer.limit());
    }
    int typeInt = buffer.get();
    Types type;
    try {
      type = Types.values()[typeInt];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new UnknownLogTypeException(typeInt);
    }
    switch (type) {
      // TODO-Cluster support more logs
      case ADD_NODE:
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.deserialize(buffer);
        return addNodeLog;
      case PHYSICAL_PLAN:
        PhysicalPlanLog physicalPlanLog = new PhysicalPlanLog();
        physicalPlanLog.deserialize(buffer);
        return physicalPlanLog;
      default:
        throw new IllegalArgumentException(type.toString());
    }
  }
}
