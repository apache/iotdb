package org.apache.iotdb.udf;

import org.apache.iotdb.udf.table.MatchingKeyState;

import org.junit.Assert;
import org.junit.Test;

public class UDAFMatchingKeyStateTest {
  @Test
  public void testSerializeDeserialize() {
    MatchingKeyState original = new MatchingKeyState();
    original.setColumnLength(2);
    original.init(1, 0.3, 0.8);
    original.add(0, 123456L, new String[] {"A1", "B2"});
    original.add(1, 123457L, new String[] {"C3", "D4"});
    original.add(2, 123458L, new String[] {"E5", "F6"});

    original.addPositivePair("0+1");
    original.addNegativePair("1+2");

    byte[] bytes = original.serialize();
    Assert.assertNotNull(bytes);

    MatchingKeyState copy = new MatchingKeyState();
    copy.deserialize(bytes);

    Assert.assertEquals(2, copy.getColumnLength());
    Assert.assertEquals(1, copy.getLabelIndex());
    Assert.assertEquals(0.3, copy.getMinSupport(), 1e-6);
    Assert.assertEquals(0.8, copy.getMinConfidence(), 1e-6);

    Assert.assertNotNull(copy.getTupleList());
    Assert.assertEquals(3, copy.getTupleList().size());
    for (int i = 0; i < copy.getTupleList().size(); i++) {
      MatchingKeyState.TupleEntry t1 = original.getTupleList().get(i);
      MatchingKeyState.TupleEntry t2 = copy.getTupleList().get(i);
      Assert.assertEquals(t1.time, t2.time);
      Assert.assertArrayEquals(t1.tuple, t2.tuple);
    }

    Assert.assertEquals(original.getPositivePairs(), copy.getPositivePairs());
    Assert.assertEquals(original.getNegativePairs(), copy.getNegativePairs());
  }
}
