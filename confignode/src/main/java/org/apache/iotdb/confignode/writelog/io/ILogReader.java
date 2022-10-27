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
package org.apache.iotdb.confignode.writelog.io;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface ILogReader {

  /** release resources occupied by this object, like file streams. */
  void close();

  /**
   * return whether there exists next log to be read.
   *
   * @return whether there exists next log to be read.
   * @throws IOException
   */
  boolean hasNext() throws FileNotFoundException;

  /**
   * return the next log read from media like a WAL file and covert it to a PhysicalPlan.
   *
   * @return the next log as a PhysicalPlan
   * @throws java.util.NoSuchElementException when there are no more logs
   */
  ConfigPhysicalPlan next() throws FileNotFoundException;
}
