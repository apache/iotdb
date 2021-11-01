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

import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log.Types;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.logtypes.LargeTestLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/** LogParser transform a ByteBuffer into a Log. */
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
    logger.debug("The log type is {}", type);
    Log log;
    switch (type) {
      case ADD_NODE:
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.deserialize(buffer);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "The last meta log index of log {} is {}", addNodeLog, addNodeLog.getMetaLogIndex());
        }
        log = addNodeLog;
        break;
      case PHYSICAL_PLAN:
        PhysicalPlanLog physicalPlanLog = new PhysicalPlanLog();
        physicalPlanLog.deserialize(buffer);
        log = physicalPlanLog;
        break;
      case CLOSE_FILE:
        CloseFileLog closeFileLog = new CloseFileLog();
        closeFileLog.deserialize(buffer);
        log = closeFileLog;
        break;
      case REMOVE_NODE:
        RemoveNodeLog removeNodeLog = new RemoveNodeLog();
        removeNodeLog.deserialize(buffer);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "The last meta log index of log {} is {}",
              removeNodeLog,
              removeNodeLog.getMetaLogIndex());
        }
        log = removeNodeLog;
        break;
      case EMPTY_CONTENT:
        EmptyContentLog emptyLog = new EmptyContentLog();
        emptyLog.deserialize(buffer);
        log = emptyLog;
        break;
      case TEST_LARGE_CONTENT:
        LargeTestLog largeLog = new LargeTestLog();
        largeLog.deserialize(buffer);
        log = largeLog;
        break;
      default:
        throw new IllegalArgumentException(type.toString());
    }
    logger.debug("Parsed a log {}", log);
    return log;
  }
}
