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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto;

import org.apache.tsfile.read.common.block.TsBlock;

import java.io.IOException;

/**
 * Interface for writing query results to external storage formats.
 *
 * <p>This interface abstracts the writing logic so that different output formats (e.g., TsFile,
 * CSV) can be supported. Each implementation handles format-specific operations like encoding,
 * schema definition, and file sealing.
 */
public interface IFormatCopyToWriter {

  /**
   * Writes a TsBlock containing query results to the target storage.
   *
   * @param tsBlock the data block to write, containing rows of query results
   * @throws Exception if write fails (e.g., IO error, encoding error)
   */
  void write(TsBlock tsBlock) throws Exception;

  /**
   * Builds a TsBlock containing metadata or summary information about the written data.
   *
   * <p>This is typically called after all data has been written to return information about the
   * output, such as file paths, row counts, or statistics.
   *
   * @return a TsBlock containing result metadata, or null if no result is needed
   */
  TsBlock buildResultTsBlock();

  /**
   * Finalizes the writing process and prepares the file for reading.
   *
   * <p>This method ensures all data is flushed to disk and necessary footer/headers are written.
   * After seal() is called, no more data should be written.
   *
   * @throws Exception if sealing fails
   */
  void seal() throws Exception;

  /**
   * Closes the writer and releases all resources.
   *
   * <p>This method should be called after seal() to ensure proper cleanup of file handles, buffers,
   * and other resources.
   *
   * @throws IOException if close fails
   */
  void close() throws IOException;
}
