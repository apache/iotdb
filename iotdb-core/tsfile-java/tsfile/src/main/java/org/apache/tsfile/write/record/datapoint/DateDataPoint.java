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

package org.apache.tsfile.write.record.datapoint;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;

public class DateDataPoint extends DataPoint {

  private static final Logger LOG = LoggerFactory.getLogger(DateDataPoint.class);

  /** actual value. */
  private LocalDate value;

  public DateDataPoint(String measurementId, LocalDate v) {
    super(TSDataType.DATE, measurementId);
    this.value = v;
  }

  @Override
  public void writeTo(long time, ChunkWriterImpl writer) throws IOException {
    if (writer == null) {
      LOG.warn("given IChunkWriter is null, do nothing and return");
      return;
    }
    writer.write(time, DateUtils.parseDateExpressionToInt(value));
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setDate(LocalDate value) {
    this.value = value;
  }
}
