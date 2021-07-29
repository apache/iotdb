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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

/** This interface defines a MeasurementMNode's operation interfaces. */
public interface IMeasurementMNode extends IMNode {
  int getMeasurementCount();

  IMeasurementSchema getSchema();

  TimeValuePair getCachedLast();

  void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime);

  void resetCache();

  long getOffset();

  void setOffset(long offset);

  String getAlias();

  TriggerExecutor getTriggerExecutor();

  void setAlias(String alias);

  void setSchema(IMeasurementSchema schema);

  void setTriggerExecutor(TriggerExecutor triggerExecutor);

  TSDataType getDataType(String measurementId);
}
