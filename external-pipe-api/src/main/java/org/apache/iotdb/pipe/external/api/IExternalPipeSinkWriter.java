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

package org.apache.iotdb.pipe.external.api;

import java.io.IOException;

/** Responsible for forwarding the operations to the sink. */
public interface IExternalPipeSinkWriter extends AutoCloseable {

  /** Initialize the writer. */
  void open() throws IOException;

  /**
   * Insert a boolean data point to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertBoolean(String sgName, String[] path, long time, boolean value) throws IOException;

  /**
   * Insert a 32-bit integer data point to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertInt32(String sgName, String[] path, long time, int value) throws IOException;

  /**
   * Insert a 64-bit integer data point to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertInt64(String sgName, String[] path, long time, long value) throws IOException;

  /**
   * Insert a float data point to the sink.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertFloat(String sgName, String[] path, long time, float value) throws IOException;

  /**
   * Insert a double data point to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertDouble(String sgName, String[] path, long time, double value) throws IOException;

  /**
   * Insert a text data point to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   *
   * @param sgName Storage-Group's name.
   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the input
   *     argument would be ["root", "a", "b", "c"].
   * @param time Timestamp of the data point. Unit ms.
   * @param value Value of the data point.
   */
  void insertText(String sgName, String[] path, long time, String value) throws IOException;

  //  /**
  //   * Insert a vector data point to the sink.
  //   *
  //   * <p>The framework will retry if this method throws an {@link IOException}.
  //   *
  //   * @param sgName Storage-Group's name.
  //   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the
  // input
  //   *     argument would be ["root", "a", "b", "c"].
  //   * @param dataTypes Datatype of each element in the vector.
  //   * @param time Timestamp of the data point. Unit ms.
  //   * @param values Value of each element in the vector.
  //   */
  //  void insertVector(String sgName, String[] path, DataType[] dataTypes, long time, Object[]
  // values)
  //      throws IOException;

  /**
   * Delete the data points whose timestamp is >= startTime and <= endTime from the sink. The
   * parameter path indicates the path of deleted points, and it may be "root.a.b.c" or "root.a.b.*"
   * or "root.**" etc.
   *
   * @param sgName Storage-Group's name.
   * @param delPath The path of deletion action. For example, sg1.*, sg1.*.*.
   * @param startTime Beginning timestamp of the deleted data points. Unit ms.
   * @param endTime Ending timestamp of the deleted data points. Unit ms.
   */
  void delete(String sgName, String delPath, long startTime, long endTime) throws IOException;

  //  /**
  //   * Handle the creation of a timeseries.
  //   *
  //   * <p>The framework will retry if this method throws an {@link IOException}.
  //   *
  //   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the
  // input
  //   *     argument would be ["root", "a", "b", "c"].
  //   * @param dataType Datatype of the timeseries.
  //   */
  //  void createTimeSeries(String[] path, DataType dataType) throws IOException;
  //
  //  /**
  //   * Handle the deletion of a timeseries.
  //   *
  //   * <p>The framework will retry if this method throws an {@link IOException}.
  //   *
  //   * @param path The parts of a path separated by '.'. For example, for a path root.a.b.c, the
  // input
  //   *     argument would be ["root", "a", "b", "c"].
  //   */
  //  void deleteTimeSeries(String[] path) throws IOException;

  /**
   * Flush the data and metadata changes to the sink.
   *
   * <p>The framework will retry if this method throws an {@link IOException}.
   */
  void flush() throws IOException;

  /** Get the status of this writer. This method should NOT throw any exception. */
  ExternalPipeSinkWriterStatus getStatus();

  /**
   * Close the writer.
   *
   * <p>The framework will NOT retry if this method throws an {@link IOException}.
   */
  @Override
  void close() throws IOException;
}
