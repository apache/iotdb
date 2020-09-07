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

public class SerializableBooleanTVListTest extends SerializableTVListTest {

  private List<Boolean> originalList;
  private SerializableBooleanTVList testList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    originalList = new ArrayList<>();
    testList = (SerializableBooleanTVList) SerializableTVList
        .newSerializableTVList(TSDataType.BOOLEAN, QUERY_ID, UNIQUE_ID, INDEX);
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  protected void generateData(int index) {
    boolean value = index % 2 == 0;
    originalList.add(value);
    testList.putBoolean(index, value);
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
      assertEquals(originalList.get(count), testList.getBoolean());
      testList.next();
      ++count;
    }
    assertEquals(ITERATION_TIMES, count);
  }
}
