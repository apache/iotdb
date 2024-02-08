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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;

public interface IAggregateReader {

  boolean hasNextFile() throws IOException;

  boolean canUseCurrentFileStatistics() throws IOException;

  Statistics currentFileStatistics() throws IOException;

  void skipCurrentFile();

  boolean hasNextChunk() throws IOException;

  boolean canUseCurrentChunkStatistics() throws IOException;

  Statistics currentChunkStatistics() throws IOException;

  void skipCurrentChunk();

  boolean hasNextPage() throws IOException;

  /** only be used without value filter */
  boolean canUseCurrentPageStatistics() throws IOException;

  /** only be used without value filter */
  Statistics currentPageStatistics() throws IOException;

  void skipCurrentPage();

  BatchData nextPage() throws IOException;

  boolean isAscending();
}
