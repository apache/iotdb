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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ShowNowPlan extends ShowPlan {
  private static final Logger logger = LoggerFactory.getLogger(ShowPlan.class);

  String ipAddress;
  String systemTime;
  String cpuLoad;
  String totalMemorySize;
  String freeMemorySize;

  public ShowNowPlan() {
    super(ShowContentType.NOW);
  }

  public ShowNowPlan(ShowContentType showContentType) {
    super(showContentType);
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.write(PhysicalPlanType.SHOW_NOW.ordinal());
    //    putString(outputStream, ipAddress);
    //    putString(outputStream, systemTime);
    //    putString(outputStream, cpuLoad);
    //    putString(outputStream, totalMemorySize);
    //    putString(outputStream, freeMemorySize);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    ipAddress = readString(buffer);
    systemTime = readString(buffer);
    cpuLoad = readString(buffer);
    totalMemorySize = readString(buffer);
    freeMemorySize = readString(buffer);
  }
}
