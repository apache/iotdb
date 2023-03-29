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

package org.apache.iotdb.commons.client;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.net.SocketException;

/**
 * This class defines the failed interfaces that thrift client needs to support so that the Thrift
 * Client can clean up the clientManager when it receives the corresponding exception.
 */
public interface ThriftClient {

  Logger logger = LoggerFactory.getLogger(ThriftClient.class);

  /** Close this connection. */
  void invalidate();

  /** Removing all pooled instances corresponding to current instance's endpoint. */
  void invalidateAll();

  /**
   * Whether to print logs when exceptions are encountered.
   *
   * @return result
   */
  boolean printLogWhenEncounterException();

  /**
   * Perform corresponding operations on ThriftClient o based on the Throwable t.
   *
   * @param t Throwable
   * @param o ThriftClient
   */
  static void resolveException(Throwable t, ThriftClient o) {
    Throwable origin = t;
    if (t instanceof InvocationTargetException) {
      origin = ((InvocationTargetException) t).getTargetException();
    }
    Throwable cur = origin;
    if (cur instanceof TException) {
      int level = 0;
      while (cur != null) {
        logger.debug(
            "level-{} Exception class {}, message {}",
            level,
            cur.getClass().getName(),
            cur.getMessage());
        cur = cur.getCause();
        level++;
      }
      o.invalidate();
    }

    Throwable rootCause = ExceptionUtils.getRootCause(origin);
    if (rootCause != null) {
      // if the exception is SocketException and its error message is Broken pipe, it means that
      // the remote node may restart and all the connection we cached before should be cleared.
      logger.debug(
          "root cause message {}, LocalizedMessage {}, ",
          rootCause.getMessage(),
          rootCause.getLocalizedMessage(),
          rootCause);
      if (isConnectionBroken(rootCause)) {
        if (o.printLogWhenEncounterException()) {
          logger.info(
              "Broken pipe error happened in sending RPC,"
                  + " we need to clear all previous cached connection",
              t);
        }
        o.invalidateAll();
      }
    }
  }

  /**
   * Determine whether the target node has gone offline once based on the cause.
   *
   * @param cause Throwable
   * @return true/false
   */
  static boolean isConnectionBroken(Throwable cause) {
    return (cause instanceof SocketException && cause.getMessage().contains("Broken pipe"))
        || (cause instanceof TTransportException
            && cause.getMessage().contains("Socket is closed by peer"));
  }
}
