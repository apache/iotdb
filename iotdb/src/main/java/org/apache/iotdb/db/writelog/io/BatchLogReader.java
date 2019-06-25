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

package org.apache.iotdb.db.writelog.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

/**
 * BatchedLogReader reads logs from a binary batch of log in the format of ByteBuffer. The
 * ByteBuffer must be readable.
 */
public class BatchLogReader implements ILogReader{

  private Iterator<PhysicalPlan> planIterator;

  public BatchLogReader(ByteBuffer buffer) {
    List<PhysicalPlan> logs = readLogs(buffer);
    this.planIterator = logs.iterator();
  }

  private List<PhysicalPlan> readLogs(ByteBuffer buffer) {
    List<PhysicalPlan> plans = new ArrayList<>();
    while (buffer.position() != buffer.limit()) {
      plans.add(PhysicalPlan.Factory.create(buffer));
    }
    return plans;
  }


  @Override
  public void close() {
  }

  @Override
  public boolean hasNext() throws IOException {
    return planIterator.hasNext();
  }

  @Override
  public PhysicalPlan next() throws IOException {
    return planIterator.next();
  }
}
