package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;

public class SerializableDoubleTVListTest extends SerializableTVListTest {

  private List<Double> originalList;
  private SerializableDoubleTVList testList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    originalList = new ArrayList<>();
    testList = (SerializableDoubleTVList) SerializableTVList
        .newSerializableTVList(TSDataType.DOUBLE, QUERY_ID, UNIQUE_ID, INDEX);
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  protected void generateData(int index) {
    originalList.add((double) index);
    testList.putDouble(index, index);
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
      assertEquals(originalList.get(count), testList.getDouble(), 0);
      testList.next();
      ++count;
    }
    assertEquals(ITERATION_TIMES, count);
  }
}
