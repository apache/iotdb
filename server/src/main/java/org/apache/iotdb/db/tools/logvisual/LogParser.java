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
 *
 */

package org.apache.iotdb.db.tools.logvisual;

import java.io.IOException;

/**
 * LogParser works as an iterator of logs.
 */
public interface LogParser {

  /**
   * return the next LogEntry or null if there are no more logs.
   */
  LogEntry next() throws IOException;

  /**
   * Release resources such as file streams.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Start the parse from the beginning. Must be called before the first call to next().
   * @throws IOException
   */
  void reset() throws IOException;
}