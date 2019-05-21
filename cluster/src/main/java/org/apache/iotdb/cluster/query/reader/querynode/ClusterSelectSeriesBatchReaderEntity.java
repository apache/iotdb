/**
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
package org.apache.iotdb.cluster.query.reader.querynode;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch reader entity for all select paths.
 */
public class ClusterSelectSeriesBatchReaderEntity {
  /**
   * All select paths
   */
  List<String> paths;

  /**
   * All select readers
   */
  List<AbstractClusterSelectSeriesBatchReader> readers;

  public ClusterSelectSeriesBatchReaderEntity() {
    paths = new ArrayList<>();
    readers = new ArrayList<>();
  }

  public void addPath(String path) {
    this.paths.add(path);
  }

  public void addReaders(AbstractClusterSelectSeriesBatchReader reader) {
    this.readers.add(reader);
  }

  public List<AbstractClusterSelectSeriesBatchReader> getAllReaders() {
    return readers;
  }

  public AbstractClusterSelectSeriesBatchReader getReaderByIndex(int index){
    return readers.get(index);
  }

  public List<String> getAllPaths() {
    return paths;
  }
}
