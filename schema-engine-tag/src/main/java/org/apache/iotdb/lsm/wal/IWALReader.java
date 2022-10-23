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
package org.apache.iotdb.lsm.wal;

import java.io.FileNotFoundException;
import java.io.IOException;

/** get records in wal file */
public interface IWALReader {

  /**
   * close resource
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * determine if there is a next record
   *
   * @return returns true if there is, else returns false
   * @throws FileNotFoundException
   */
  boolean hasNext() throws FileNotFoundException;

  /**
   * return the next record
   *
   * @throws FileNotFoundException
   */
  IWALRecord next() throws FileNotFoundException;
}
