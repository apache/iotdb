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
package org.apache.iotdb.db.fileindex.impl;

import java.util.Collections;
import java.util.Map;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.fileindex.FileIndexEntries;
import org.apache.iotdb.db.fileindex.FileTimeIndexer;
import org.apache.iotdb.db.fileindex.TimeIndexEntry;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class LoadAllDeviceTimeIndexer implements FileTimeIndexer {

  public LoadAllDeviceTimeIndexer() {

  }

  @Override
  public boolean init() {
    return false;
  }

  @Override
  public boolean begin() {
    return false;
  }

  @Override
  public boolean end() {
    return false;
  }

  @Override
  public boolean createIndexForPath(FileIndexEntries fileIndexEntries) {
    return false;
  }

  @Override
  public boolean deleteIndexOfPath(FileIndexEntries fileIndexEntries) {
    return false;
  }

  @Override
  public Map<String, TimeIndexEntry[]> filterByPath(PartialPath path, Filter timeFilter) {
    return Collections.emptyMap();
  }
}
