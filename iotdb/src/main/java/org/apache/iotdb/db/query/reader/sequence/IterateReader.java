/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.sequence;

import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * get data sequentially from the reader list.
 */
public class IterateReader implements IAggregateReader {

  protected List<IAggregateReader> seqResourceSeriesReaderList;
  protected boolean curReaderInitialized;
  protected int nextSeriesReaderIndex;
  protected IAggregateReader currentSeriesReader;

  public IterateReader() {
    this.seqResourceSeriesReaderList = new ArrayList<>();
    this.curReaderInitialized = false;
    this.nextSeriesReaderIndex = 0;
  }

  @Override
  public boolean hasNext() throws IOException {

    if (curReaderInitialized && currentSeriesReader.hasNext()) {
      return true;
    } else {
      curReaderInitialized = false;
    }

    while (nextSeriesReaderIndex < seqResourceSeriesReaderList.size()) {
      currentSeriesReader = seqResourceSeriesReaderList.get(nextSeriesReaderIndex++);
      if (currentSeriesReader.hasNext()) {
        curReaderInitialized = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    for (IBatchReader seriesReader : seqResourceSeriesReaderList) {
      seriesReader.close();
    }
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return currentSeriesReader.nextBatch();
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return currentSeriesReader.nextPageHeader();
  }

  @Override
  public void skipPageData() throws IOException {
    currentSeriesReader.skipPageData();
  }
}
