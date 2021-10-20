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
package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Set;

public abstract class AbstractMultPointReader implements IPointReader {

  public abstract boolean hasNextTimeValuePair(String fullPath) throws IOException;

  public abstract TimeValuePair nextTimeValuePair(String fullPath) throws IOException;

  public abstract Set<String> getAllPaths();

  /**
   * do not support this method
   *
   * @return only false
   * @throws IOException
   */
  @Override
  @Deprecated
  public boolean hasNextTimeValuePair() throws IOException {
    return false;
  }

  /**
   * do not support this method
   *
   * @return only null
   * @throws IOException
   */
  @Override
  @Deprecated
  public TimeValuePair nextTimeValuePair() throws IOException {
    return null;
  }

  /**
   * do not support this method
   *
   * @return only null
   * @throws IOException
   */
  @Override
  @Deprecated
  public TimeValuePair currentTimeValuePair() throws IOException {
    return null;
  }
}
