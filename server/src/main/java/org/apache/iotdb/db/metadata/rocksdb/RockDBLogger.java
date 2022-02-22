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
package org.apache.iotdb.db.metadata.rocksdb;

import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;

public class RockDBLogger extends Logger {
  private final org.slf4j.Logger logger;

  public RockDBLogger(Options options, org.slf4j.Logger logger) {
    super(options);
    this.logger = logger;
  }

  public RockDBLogger(DBOptions options, org.slf4j.Logger logger) {
    super(options);
    this.logger = logger;
  }

  @Override
  protected void log(InfoLogLevel infoLogLevel, String logMsg) {
    switch (infoLogLevel) {
      case DEBUG_LEVEL:
        logger.debug(logMsg);
        break;
      case NUM_INFO_LOG_LEVELS:
      case INFO_LEVEL:
        logger.info(logMsg);
        break;
      case WARN_LEVEL:
        logger.warn(logMsg);
        break;
      case ERROR_LEVEL:
      case FATAL_LEVEL:
      case HEADER_LEVEL:
        logger.error(logMsg);
      default:
        break;
    }
  }
}
