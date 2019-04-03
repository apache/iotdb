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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.query.reader.FakedSeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.junit.Assert;
import org.junit.Test;

public class SequenceDataReaderByTimestampTest {

  /**
   * Test hasNext() and getValueInTimestamp(long timestamp) method in
   * SequenceDataReaderByTimestamp.
   */
  @Test
  public void text() throws IOException {
    List<EngineReaderByTimeStamp> readers = new ArrayList<>();
    FakedSeriesReaderByTimestamp sealedTsFile = new FakedSeriesReaderByTimestamp(100, 1000, 7, 11);
    FakedSeriesReaderByTimestamp unsealedTsFile = new FakedSeriesReaderByTimestamp(100 + 1005 * 7,
        100, 17, 3);
    FakedSeriesReaderByTimestamp dataInMemory = new FakedSeriesReaderByTimestamp(
        100 + 1005 * 7 + 100 * 17, 60, 19, 23);
    readers.add(sealedTsFile);
    readers.add(unsealedTsFile);
    readers.add(dataInMemory);
    SequenceDataReaderByTimestamp sequenceReader = new SequenceDataReaderByTimestamp(readers);

    long startTime = 100l;
    long endTime = 100 + 1005 * 7 + 100 * 17 + 59 * 19;
    Random random = new Random();
    for (long time = startTime - 50; time < endTime + 50; time++) {
      if (time < endTime) {
        Assert.assertTrue(sequenceReader.hasNext());
      }
      time += 1 + random.nextInt(10);
      Object value = sequenceReader.getValueInTimestamp(time);
      if (time < 100) {
        Assert.assertNull(value);
      }
      //sealed tsfile
      else if (time < 100 + 1005 * 7) {
        if ((time - 100) % 7 != 0 || time > 100 + 999 * 7) {
          Assert.assertNull(value);
        } else {
          Assert.assertEquals(time % 11, value);
        }
      }
      //unsealed tsfile
      else if (time < 100 + 1005 * 7 + 100 * 17) {
        if ((time - (100 + 1005 * 7)) % 17 != 0) {
          Assert.assertNull(value);
        } else {
          Assert.assertEquals(time % 3, value);
        }
      }
      //memory data
      else if (time < 100 + 1005 * 7 + 100 * 17 + 60 * 19) {
        if ((time - (100 + 1005 * 7 + 100 * 17)) % 19 != 0) {
          Assert.assertNull(value);
        } else {
          Assert.assertEquals(time % 23, value);
        }
      } else {
        Assert.assertNull(value);
      }
    }
  }

}
