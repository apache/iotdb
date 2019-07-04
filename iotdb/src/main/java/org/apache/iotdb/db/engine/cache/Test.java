package org.apache.iotdb.db.engine.cache;

import java.io.File;
import java.util.HashMap;
import java.util.Random;
import org.apache.iotdb.tsfile.read.common.Path;

public class Test {

  public static void main(String[] args) {
    File tmp = new File("");
    Path path = new Path("d.s");
    Random random = new Random();

    HashMap<String, String> stringHashMap = new HashMap<>(10_000_000);
    HashMap<Long, String> objectHashMap = new HashMap<>(10_000_000);
    for (int i = 0; i < 10_000_000; i++) {
      stringHashMap.put(genStr(), "abc");
      objectHashMap.put(random.nextLong(), "abc");
    }

    System.out.println("init data finish!");
    long t1 = System.currentTimeMillis();
    for (String str : stringHashMap.keySet()) {
      stringHashMap.get(str);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("str map time = " + (t2 - t1) + "ms");
    long t3 = System.currentTimeMillis();
    for (Long obj : objectHashMap.keySet()) {
      objectHashMap.get(obj);
    }
    long t4 = System.currentTimeMillis();
    System.out
        .println("obj map time = " + (t4 - t3) + "ms" + ". obj.size = " + objectHashMap.size());

    t3 = System.currentTimeMillis();
    for (Long obj : objectHashMap.keySet()) {
      objectHashMap.get(obj);
    }
    t4 = System.currentTimeMillis();
    System.out.println("obj map time = " + (t4 - t3) + "ms");
    t1 = System.currentTimeMillis();
    for (String str : stringHashMap.keySet()) {
      stringHashMap.get(str);
    }
    t2 = System.currentTimeMillis();
    System.out.println("str map time = " + (t2 - t1) + "ms");


  }


  static String genStr() {
    StringBuilder builder = new StringBuilder();
    Random random = new Random();
    for (int j = 0; j < 32; j++) {
      char ch = (char) (random.nextInt(27) + 'a');
      builder.append(ch);
    }
    return builder.reverse().toString();
  }

}
