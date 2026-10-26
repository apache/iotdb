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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

interface DeviceTaskRunCursor extends Closeable {

  boolean hasCurrentDeviceTask();

  ExternalTsFileDeviceQueryTask getCurrentDeviceTask();

  void advance() throws IOException;

  class DiskDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final DataInputStream inputStream;
    private int remainingDeviceTasks;
    private ExternalTsFileDeviceQueryTask currentDeviceTask;

    DiskDeviceTaskRunCursor(Path runFile) throws IOException {
      DataInputStream inputStream =
          new DataInputStream(new BufferedInputStream(Files.newInputStream(runFile)));
      try {
        this.inputStream = inputStream;
        this.remainingDeviceTasks = ReadWriteIOUtils.readInt(inputStream);
        advance();
      } catch (IOException | RuntimeException e) {
        try {
          inputStream.close();
        } catch (IOException closeException) {
          e.addSuppressed(closeException);
        }
        throw e;
      }
    }

    @Override
    public void advance() throws IOException {
      if (remainingDeviceTasks <= 0) {
        currentDeviceTask = null;
        return;
      }
      remainingDeviceTasks--;
      currentDeviceTask = ExternalTsFileDeviceQueryTask.deserialize(inputStream);
    }

    @Override
    public boolean hasCurrentDeviceTask() {
      return currentDeviceTask != null;
    }

    @Override
    public ExternalTsFileDeviceQueryTask getCurrentDeviceTask() {
      return currentDeviceTask;
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }

  class MemoryDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final List<ExternalTsFileDeviceQueryTask> deviceTasks;
    private int nextIndex;
    private ExternalTsFileDeviceQueryTask currentDeviceTask;

    MemoryDeviceTaskRunCursor(List<ExternalTsFileDeviceQueryTask> deviceTasks) {
      this.deviceTasks = deviceTasks;
      advance();
    }

    @Override
    public void advance() {
      if (nextIndex >= deviceTasks.size()) {
        currentDeviceTask = null;
        return;
      }
      currentDeviceTask = deviceTasks.get(nextIndex++);
    }

    @Override
    public boolean hasCurrentDeviceTask() {
      return currentDeviceTask != null;
    }

    @Override
    public ExternalTsFileDeviceQueryTask getCurrentDeviceTask() {
      return currentDeviceTask;
    }

    @Override
    public void close() {
      currentDeviceTask = null;
    }
  }
}
