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
package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

public class FileSeriesIAggregateReader implements IAggregateReader {

  private FileSeriesReader fileSeriesReader;

  public FileSeriesIAggregateReader(FileSeriesReader fileSeriesReader){
    this.fileSeriesReader = fileSeriesReader;
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return fileSeriesReader.nextPageHeader();
  }

  @Override
  public void skipPageData() {
    fileSeriesReader.skipPageData();
  }

  @Override
  public boolean hasNext() throws IOException {
    return fileSeriesReader.hasNextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return fileSeriesReader.nextBatch();
  }

  @Override
  public void close() throws IOException {
    fileSeriesReader.close();
  }
}
