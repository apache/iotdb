package org.apache.iotdb.db.nvm;

import java.util.HashMap;
import java.util.Map;

public class PerfMonitor {

  private static Map<String, Perf> perfMap = new HashMap<>();

  public synchronized static void add(String name, long timeLen) {
    Perf perf;
    if (!perfMap.containsKey(name)) {
      perf = new Perf(name);
      perfMap.put(name, perf);
    } else {
      perf = perfMap.get(name);
    }

    perf.add(timeLen);
  }

  public static void output() {
    perfMap.values().forEach(perf -> System.out.println(perf));
  }

  private static class Perf {
    String name;
    long timeLen = 0;
    int count = 0;

    public Perf(String name) {
      this.name = name;
    }

    public void add(long tl) {
      timeLen += tl;
      count++;
    }

    @Override
    public String toString() {
      return name + ":\t\t\t" + timeLen + "\t\t\t" + count;
    }
  }
}
