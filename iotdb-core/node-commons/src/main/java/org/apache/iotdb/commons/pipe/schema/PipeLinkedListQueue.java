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

package org.apache.iotdb.commons.pipe.schema;

import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: implement the snapshot logic
public abstract class PipeLinkedListQueue<E> implements SnapshotProcessor {

  private final List<LinkedListMessageQueue<E>> configPlanMessageQueues = new ArrayList<>();

  protected void listen(E element) {
    if (!configPlanMessageQueues.isEmpty()) {
      LinkedListMessageQueue<E> lastMessageQueue =
          configPlanMessageQueues.get(configPlanMessageQueues.size() - 1);
      if (lastMessageQueue != null) {
        lastMessageQueue.add(element);
      }
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return false;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    // Do nothing
  }
}
