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

package org.apache.iotdb.db.pipe.external;

import org.apache.iotdb.db.pipe.external.operation.Operation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link ExternalPipeSource} is bind with an instance of TsFilePipe and an instance of {@link
 * ExternalPipe}.
 *
 * <p>It provides a virtual queue of {@link Operation} on top of the TsFile(s) from TsFilePipe, so
 * that the {@link ExternalPipe} don't have to worry about the conversion from TsFile(s) to {@link
 * Operation}s.
 */
public class ExternalPipeSource {

  //  TsFilePipe getTsFilePipe() {
  // TODO: implement
  //  }

  /**
   * Serialize the state for recovery.
   *
   * @return Serialized state.
   */
  ByteBuffer serialize() {
    throw new UnsupportedOperationException();
  }

  /**
   * Deserialize the serialized states and resume the previous state.
   *
   * @param byteBuffer Serialized state.
   */
  void deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the timeseries groups. A group does not have overlap with any other group. The number of
   * returned groups should not exceed the given max value.
   */
  List<ExternalPipeTimeseriesGroup> getExternalPipeTimeseriesGroups(int max) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the index of first available record. This method will be called at the initialization of an
   * {@link ExternalPipe} by external pipe workers.
   */
  long getFirstAvailableIndex(ExternalPipeTimeseriesGroup group) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the last committed index. This method should be called at the recovery of an {@link
   * ExternalPipe}.
   *
   * @return The last committed index of the given group. -1 if the group has never been committed.
   */
  long getLastCommittedIndex(ExternalPipeTimeseriesGroup group) {
    throw new UnsupportedOperationException();
  }

  /**
   * Block until the given group have available operations.
   *
   * @param group The group.
   */
  void waitForOperations(ExternalPipeTimeseriesGroup group, long index) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get certain amount of operations of the given group starting from the given index.
   *
   * @param group The group.
   * @param index The start index.
   * @param length The required amount of {@link Operation}.
   * @return A list of operations sorted by index in ascending order, starting from the given index.
   *     The size of the list will not exceed the given length.
   * @throws IOException When the given index is not available or an IO error happens.
   */
  List<Operation> getOperations(ExternalPipeTimeseriesGroup group, long index, int length)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * This method should be called by an {@link ExternalPipe}, indicating that data before the given
   * index has been successfully sent to the sink.
   */
  void commit(ExternalPipeTimeseriesGroup group, long index) {
    throw new UnsupportedOperationException();
  }
}
