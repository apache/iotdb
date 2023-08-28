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

package org.apache.iotdb.consensus.natraft.protocol.log;

import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.natraft.exception.UnknownLogTypeException;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry.Types;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.EmptyEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.RequestEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/** LogParser transform a ByteBuffer into a Entry. */
public class LogParser {

  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
  private static final LogParser INSTANCE = new LogParser();

  private LogParser() {
    // singleton class
  }

  public static LogParser getINSTANCE() {
    return INSTANCE;
  }

  public Entry parse(ByteBuffer buffer, IStateMachine stateMachine) throws UnknownLogTypeException {
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
    int startPos = buffer.position();
    Entry log;
    switch (type) {
      case CLIENT_REQUEST:
        RequestEntry requestLog = new RequestEntry();
        requestLog.deserialize(buffer);
        if (stateMachine != null) {
          try {
            requestLog.setRequest(stateMachine.deserializeRequest(requestLog.getRequest()));
          } catch (RuntimeException e) {
            logger.error("Cannot deserialize request {} with statemachine", requestLog);
            throw e;
          }
        }
        log = requestLog;
        break;
      case EMPTY:
        EmptyEntry emptyLog = new EmptyEntry();
        emptyLog.deserialize(buffer);
        log = emptyLog;
        break;
      default:
        throw new IllegalArgumentException(type.toString());
    }
    log.setByteSize(buffer.position() - startPos);
    logger.debug("Parsed a log {}", log);
    return log;
  }
}
