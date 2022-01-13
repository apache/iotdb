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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongPageReader extends PageReader {

  public LongPageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder, Filter filter) {
    super(pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public LongPageReader(PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder, Filter filter) {
    super(pageHeader, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);

      long aLong = valueDecoder.readLong(valueBuffer);
      pageData.putLong(timestamp, aLong);
    }
    return pageData.flip();
  }

}
