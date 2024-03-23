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

package org.apache.iotdb.isession.subscription;

import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

public class EnrichedRowRecord {

  private final String topicName;
  private final List<String> columnNameList;
  private final List<String> columnTypeList;
  private final List<RowRecord> records;

  public EnrichedRowRecord(EnrichedTablets enrichedTablets) {
    this.topicName = enrichedTablets.getTopicName();
    this.columnNameList = enrichedTablets.generateColumnNameList();
    this.columnTypeList = enrichedTablets.generateColumnTypeList();
    this.records = enrichedTablets.generateRecords();
  }

  public String getTopicName() {
    return topicName;
  }

  public List<String> getColumnNameList() {
    return columnNameList;
  }

  public List<String> getColumnTypeList() {
    return columnTypeList;
  }

  public List<RowRecord> getRecords() {
    return records;
  }
}
