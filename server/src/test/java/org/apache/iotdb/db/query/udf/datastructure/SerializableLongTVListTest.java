package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;

public class SerializableLongTVListTest extends SerializableTVListTest {

  private List<Long> originalList;
  private SerializableLongTVList testList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    originalList = new ArrayList<>();
    testList = (SerializableLongTVList) SerializableTVList
        .newSerializableTVList(TSDataType.INT64, QUERY_ID, UNIQUE_ID, INDEX);
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  protected void generateData(int index) {
    originalList.add((long) index);
    testList.putLong(index, index);
  }

  @Override
  protected void serializeAndDeserializeOnce() {
    try {
      testList.serialize();
    } catch (IOException e) {
      fail();
    }
    assertTrue(testList.isEmpty());
    try {
      testList.deserialize();
    } catch (IOException e) {
      fail();
    }
    int count = 0;
    while (testList.hasCurrent()) {
      assertEquals(count, testList.currentTime());
      assertEquals(originalList.get(count), testList.getLong(), 0);
      testList.next();
      ++count;
    }
    assertEquals(ITERATION_TIMES, count);
  }
}
