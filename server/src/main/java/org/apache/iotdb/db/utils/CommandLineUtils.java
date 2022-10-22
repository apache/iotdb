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
package org.apache.iotdb.db.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineUtils {

  private static final Logger logger = LoggerFactory.getLogger(CommandLineUtils.class);

  /**
   * Check required arg from {@link CommandLine}
   *
   * @param arg arg
   * @param name name
   * @param commandLine commandLine after parse
   * @return required value
   * @throws ParseException Required value not provided
   */
  public static String checkRequiredArg(String arg, String name, CommandLine commandLine)
      throws ParseException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      String msg = String.format("Required values for option '%s' not provided", name);
      logger.info(msg);
      logger.info("Use -help for more information");
      throw new ParseException(msg);
    }
    return str;
  }
}
