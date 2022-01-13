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
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.exception.PipeException;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;

public class TsFilePipe implements Pipe {
  private static final String SERIALIZE_SPLIT_TOKEN = ",";

  private final long createTime;

  private final String name;
  private final IoTDBPipeSink pipeSink;
  private final long dataStartTimestamp;
  private final boolean syncDelOp;

  private PipeStatus status;

  public TsFilePipe(
      String name, IoTDBPipeSink pipeSink, long dataStartTimestamp, boolean syncDelOp) {
    this.name = name;
    this.pipeSink = pipeSink;
    this.dataStartTimestamp = dataStartTimestamp;
    this.syncDelOp = syncDelOp;

    createTime = DatetimeUtils.currentTime();
  }

  public TsFilePipe(
      long createTime,
      String name,
      IoTDBPipeSink pipeSink,
      long dataStartTimestamp,
      boolean syncDelOp) {
    this.createTime = createTime;
    this.name = name;
    this.pipeSink = pipeSink;
    this.dataStartTimestamp = dataStartTimestamp;
    this.syncDelOp = syncDelOp;
    this.status = PipeStatus.STOP;
  }

  @Override
  public void start() {
    status = PipeStatus.RUNNING;
  }

  @Override
  public void stop() {
    status = PipeStatus.STOP;
  }

  @Override
  public void drop() {
    status = PipeStatus.DROP;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public PipeSink getPipeSink() {
    return pipeSink;
  }

  @Override
  public long getCreateTime() {
    return createTime;
  }

  @Override
  public PipeStatus getStatus() {
    return status;
  }
}
