package cn.edu.tsinghua.tsfile.common.utils;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class PairTest {

  @Test
  public void testEqualsObject() {
    Pair<String, Integer> p1 = new Pair<String, Integer>("a", 123123);
    Pair<String, Integer> p2 = new Pair<String, Integer>("a", 123123);
    assertTrue(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", null);
    p2 = new Pair<String, Integer>("a", 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", 123123);
    p2 = new Pair<String, Integer>("a", null);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = new Pair<String, Integer>("a", 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", 123123);
    p2 = new Pair<String, Integer>(null, 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = null;
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = new Pair<String, Integer>(null, 123123);
    Map<Pair<String, Integer>, Integer> map = new HashMap<Pair<String, Integer>, Integer>();
    map.put(p1, 1);
    assertTrue(map.containsKey(p2));
    assertTrue(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", null);
    p2 = new Pair<String, Integer>("a", null);
    assertTrue(p1.equals(p2));
    assertTrue(p1.equals(p1));
    assertFalse(p1.equals(new Integer(1)));
  }

  @Test
  public void testToString() {
    Pair<String, Integer> p1 = new Pair<String, Integer>("a", 123123);
    assertEquals("<a,123123>", p1.toString());
    Pair<Float, Double> p2 = new Pair<Float, Double>(32.5f, 123.123d);
    assertEquals("<32.5,123.123>", p2.toString());
  }

}
