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
package org.apache.iotdb.db.query.reader.fileRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

/**
 * To read a sequence TsFile's on-disk data by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} based on the data reader {@link FileSeriesReaderByTimestamp}.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceReaderByTimestamp}.
 */
public class FileSeriesReaderByTimestampAdapter implements IReaderByTimestamp {

  private FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp;

  public FileSeriesReaderByTimestampAdapter(
      FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp) {
    this.fileSeriesReaderByTimestamp = fileSeriesReaderByTimestamp;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    return fileSeriesReaderByTimestamp.getValueInTimestamp(timestamp);
  }

  @Override
  public boolean hasNext() throws IOException {
    return fileSeriesReaderByTimestamp.hasNext();
  }
}
