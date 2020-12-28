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

package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.query.udf.datastructure.tv.SerializableFloatTVList;
import org.apache.iotdb.db.query.udf.datastructure.tv.SerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;

public class SerializableFloatTVListTest extends SerializableTVListTest {

  private List<Float> originalList;
  private SerializableFloatTVList testList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    originalList = new ArrayList<>();
    testList = (SerializableFloatTVList) SerializableTVList
        .newSerializableTVList(TSDataType.FLOAT, QUERY_ID);
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  protected void generateData(int index) {
    originalList.add((float) index);
    testList.putFloat(index, index);
  }

  @Override
  protected void serializeAndDeserializeOnce() {
    try {
      testList.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      testList.deserialize();
    } catch (IOException e) {
      fail();
    }
    int count = 0;
    while (testList.hasCurrent()) {
      assertEquals(count, testList.currentTime());
      assertEquals(originalList.get(count), testList.getFloat(), 0);
      testList.next();
      ++count;
    }
    assertEquals(ITERATION_TIMES, count);
  }
}
