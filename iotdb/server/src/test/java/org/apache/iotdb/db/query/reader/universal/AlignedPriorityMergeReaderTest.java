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
package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsVector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlignedPriorityMergeReaderTest {

  /**
   * TimeValuePair with high priority needs to be filled [10, null, null] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 2, 3]
   */
  @Test
  public void test1() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(2, v.getValue().getVector()[1].getInt());
    assertEquals(3, v.getValue().getVector()[2].getInt());
  }

  /**
   * TimeValuePair with high priority needs to be filled [10, 20, null] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 20, 3]
   */
  @Test
  public void test2() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    vector[1] = new TsInt(20);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(20, v.getValue().getVector()[1].getInt());
    assertEquals(3, v.getValue().getVector()[2].getInt());
  }

  /**
   * TimeValuePair with high priority needs to be filled [10, 20, 30] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 20, 30]
   */
  @Test
  public void test3() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    vector[1] = new TsInt(20);
    vector[2] = new TsInt(30);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(20, v.getValue().getVector()[1].getInt());
    assertEquals(30, v.getValue().getVector()[2].getInt());
  }
}
