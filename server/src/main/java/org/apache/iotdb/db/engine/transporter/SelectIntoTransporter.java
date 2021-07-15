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

package org.apache.iotdb.db.engine.transporter;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;

public class SelectIntoTransporter {

  private final QueryDataSet queryDataSet;
  private final List<PartialPath> intoPaths;
  private final int fetchSize;

  public SelectIntoTransporter(
      QueryDataSet queryDataSet, List<PartialPath> intoPaths, int fetchSize) {
    this.queryDataSet = queryDataSet;
    this.intoPaths = intoPaths;
    this.fetchSize = fetchSize;
  }

  public void transport() throws IoTDBException, IOException {
    while (queryDataSet.hasNext()) {
      transportTablets();
    }
  }

  private void transportTablets() {}
}
