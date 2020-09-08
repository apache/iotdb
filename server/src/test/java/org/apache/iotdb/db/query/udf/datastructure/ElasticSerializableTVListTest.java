package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSerializableTVListTest extends SerializableListTest {

  private static final float MEMORY_USAGE_LIMIT_IN_MB = 1f;
  private static final int CACHE_SIZE = 3;

  private ElasticSerializableTVList tvList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

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
    testOrderedAccessByDataPointIterator(dataType);
  }

  private void initESTVList(TSDataType dataType) {
    try {
      tvList = new ElasticSerializableTVList(dataType, QUERY_ID, UNIQUE_ID,
          MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(0, tvList.size());
  }

  private void testPut(TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putInt(i, i);
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putLong(i, i);
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putFloat(i, i);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putDouble(i, i);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putBoolean(i, i % 2 == 0);
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
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
            assertEquals(i, tvList.getInt(i));
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getLong(i));
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getFloat(i), 0);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getDouble(i), 0);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i % 2 == 0, tvList.getBoolean(i));
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(Binary.valueOf(String.valueOf(i)), tvList.getBinary(i));
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessByDataPointIterator(TSDataType dataType) {
    int count = 0;
    DataPointIterator iterator = tvList.getDataPointIterator();
    try {
      switch (dataType) {
        case INT32:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextInt());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentInt());
            ++count;
          }
          break;
        case INT64:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextLong());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentLong());
            ++count;
          }
          break;
        case FLOAT:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextFloat(), 0);
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentFloat(), 0);
            ++count;
          }
          break;
        case DOUBLE:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextDouble(), 0);
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentDouble(), 0);
            ++count;
          }
          break;
        case BOOLEAN:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count % 2 == 0, iterator.nextBoolean());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count % 2 == 0, iterator.currentBoolean());
            ++count;
          }
          break;
        case TEXT:
          while (iterator.hasNextPoint()) {
            Binary value = Binary.valueOf(String.valueOf(count));
            assertEquals(count, iterator.nextTime());
            assertEquals(value, iterator.nextBinary());
            assertEquals(value.getStringValue(), iterator.nextString());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(value, iterator.currentBinary());
            assertEquals(value.getStringValue(), iterator.currentString());
            ++count;
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, count);
  }
}
