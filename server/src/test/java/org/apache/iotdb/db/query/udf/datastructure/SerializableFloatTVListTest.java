package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        .newSerializableTVList(TSDataType.FLOAT, QUERY_ID, UNIQUE_ID, INDEX);
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
    assertTrue(testList.isEmpty());
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
