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
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.sender.service.SenderService;
import org.apache.iotdb.db.sync.transport.client.ITransportClient;

/**
 * Pipe is the abstract of a sync task, and a data source for {@linkplain ITransportClient}. When
 * the pipe is started, it will collect history data and real time data from IoTDB continuously, and
 * record it. A {@linkplain ITransportClient} can take PipeData from a pipe, just like take data
 * from a BlockingQueue.
 */
public interface Pipe {
  /**
   * Start this pipe, so it can collect data from IoTDB continuously.
   *
   * @throws PipeException This pipe is dropped or some inside error happens.
   */
  void start() throws PipeException;

  /**
   * Stop this pipe, but it will not stop collecting data from IoTDB.
   *
   * @throws PipeException This pipe is dropped or some inside error happens.
   */
  void stop() throws PipeException;

  /**
   * Drop this pipe, delete all information about this pipe on disk.
   *
   * @throws PipeException Some inside error happens(such as IOException about disk).
   */
  void drop() throws PipeException;

  /**
   * Close this pipe, stop collecting data from IoTDB, but do not delete information about this pipe
   * on disk. Used for {@linkplain SenderService#shutdown(long)}. Do not change the status of this
   * pipe.
   *
   * @throws PipeException Some inside error happens(such as IOException about disk).
   */
  void close() throws PipeException;

  /**
   * Get the name of this pipe.
   *
   * @return The name of this pipe.
   */
  String getName();

  /**
   * Get the {@linkplain PipeSink} of this pipe.
   *
   * @return The {@linkplain PipeSink} of this pipe.
   */
  PipeSink getPipeSink();

  /**
   * Get the creation time of this pipe with unit of {@linkplain
   * IoTDBConfig}.getTimestampPrecision() .
   *
   * @return A time with unit of {@linkplain IoTDBConfig#getTimestampPrecision()}.
   */
  long getCreateTime();

  /**
   * Get the {@linkplain Pipe.PipeStatus} of this pipe. When a pipe is created, the status should be
   * {@linkplain PipeStatus#STOP}
   *
   * @return The Status of this pipe.
   */
  PipeStatus getStatus();

  /**
   * Used for {@linkplain ITransportClient} to take one {@linkplain PipeData} from this pipe. If
   * there is no new data in this pipe, the method will block the thread until there is a new one.
   *
   * @return A {@linkplain PipeData}.
   * @throws InterruptedException Be Interrupted when waiting for new {@linkplain PipeData}.
   */
  PipeData take() throws InterruptedException;

  /**
   * Used for {@linkplain ITransportClient} to commit all {@linkplain PipeData}s which are taken but
   * not be committed yet.
   */
  void commit();

  // a new pipe should be stop status
  enum PipeStatus {
    RUNNING(0),
    STOP(1),
    DROP(2);

    private int value;

    PipeStatus(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static PipeStatus getByValue(int value) {
      for (PipeStatus x : values()) {
        if (x.getValue() == value) {
          return x;
        }
      }
      return null;
    }
  }
}
