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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.datastructure.SerializableList;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.type.Binary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticSerializableTVListTest extends SerializableListTest {

  private ElasticSerializableTVList tvList;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testESIntTVList() {
    testESTVList(TSDataType.INT32);
  }

  @Test
  public void testESLongTVList() {
    testESTVList(TSDataType.INT64);
  }

  @Test
  public void testESFloatTVList() {
    testESTVList(TSDataType.FLOAT);
  }

  @Test
  public void testESDoubleTVList() {
    testESTVList(TSDataType.DOUBLE);
  }

  @Test
  public void testESTextTVList() {
    testESTVList(TSDataType.TEXT);
  }

  @Test
  public void testESBooleanTVList() {
    testESTVList(TSDataType.BOOLEAN);
  }

  private void testESTVList(TSDataType dataType) {
    initESTVList(dataType);

    testPut(dataType);

    testOrderedAccessByIndex(dataType);
  }

  private void initESTVList(TSDataType dataType) {
    tvList =
        ElasticSerializableTVList.newElasticSerializableTVList(
            dataType, QUERY_ID, MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    assertEquals(0, tvList.size());
  }

  private void testPut(TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putInt(i, i);
            }
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putLong(i, i);
            }
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putFloat(i, i);
            }
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putDouble(i, i);
            }
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putBoolean(i, i % 2 == 0);
            }
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            if (i % 7 == 0) {
              tvList.putNull(i);
            } else {
              tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
            }
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, tvList.size());
  }

  private void testOrderedAccessByIndex(TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(i, tvList.getInt(i));
            }
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(i, tvList.getLong(i));
            }
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(i, tvList.getFloat(i), 0);
            }
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(i, tvList.getDouble(i), 0);
            }
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(i % 2 == 0, tvList.getBoolean(i));
            }
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            if (i % 7 == 0) {
              assertTrue(tvList.isNull(i));
            } else {
              assertFalse(tvList.isNull(i));
              assertEquals(
                  org.apache.iotdb.tsfile.utils.Binary.valueOf(String.valueOf(i)),
                  tvList.getBinary(i));
            }
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testMemoryControl() {
    initESTVList(TSDataType.TEXT);

    int byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 2;
    int byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 8;
    Random random = new Random();

    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          tvList.putNull(i);
        } else {
          tvList.putBinary(
              i,
              Binary.valueOf(
                  generateRandomString(
                      byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin))));
        }
      }
      LayerPointReader reader = tvList.constructPointReaderUsingTrivialEvictionStrategy();
      int index = 0;
      while (reader.next()) {
        if (index % 7 == 0) {
          assertTrue(reader.isCurrentNull());
        } else {
          int length = reader.currentBinary().getLength();
          assertTrue(byteLengthMin <= length && length < byteLengthMax);
        }
        reader.readyForNext();
        index++;
      }

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 16;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 32;
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          tvList.putNull(i);
        } else {
          tvList.putBinary(
              i,
              Binary.valueOf(
                  generateRandomString(
                      byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin))));
        }
      }
      index = 0;
      while (reader.next()) {
        if (index % 7 == 0) {
          assertTrue(reader.isCurrentNull());
        } else {
          int length = reader.currentBinary().getLength();
          assertTrue(byteLengthMin <= length && length < byteLengthMax);
        }
        reader.readyForNext();
        index++;
      }

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 256;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 512;
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          tvList.putNull(i);
        } else {
          tvList.putBinary(
              i,
              Binary.valueOf(
                  generateRandomString(
                      byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin))));
        }
      }
      index = 0;
      while (reader.next()) {
        if (index % 7 == 0) {
          assertTrue(reader.isCurrentNull());
        } else {
          int length = reader.currentBinary().getLength();
          assertTrue(byteLengthMin <= length && length < byteLengthMax);
        }
        reader.readyForNext();
        index++;
      }

      index = 0;
      for (int i = 0; i < 2 * ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          tvList.putNull(i);
        } else {
          tvList.putBinary(
              i,
              Binary.valueOf(
                  generateRandomString(
                      byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin))));
        }
        reader.next();
        if (index % 7 == 0) {
          assertTrue(reader.isCurrentNull());
        } else {
          int length = reader.currentBinary().getLength();
          assertTrue(byteLengthMin <= length && length < byteLengthMax);
        }
        reader.readyForNext();
        index++;
      }

      assertEquals(ITERATION_TIMES * 5, tvList.size());
    } catch (QueryProcessException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private String generateRandomString(int length) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < length; ++i) {
      stringBuilder.append('.');
    }
    return stringBuilder.toString();
  }
}
