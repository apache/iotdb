package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

public class Main {
  public static void main(String[] args) {
    for (String s : dataSet) {
      System.out.println(s + ";");
    }
  }

  private static final String[] dataSet =
      new String[] {
        "CREATE DATABASE root.testWithoutAllNull",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1) " + "values(6, 26)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s2) " + "values(7, false)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(9, 29, true)",
        "flush",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(10, 20, true)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2,s3) "
            + "values(11, 21, false, 11.1)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(12, 22, true)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2,s3) "
            + "values(13, 23, false, 33.3)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s3) " + "values(14, 24, 44.4)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s2,s3) " + "values(15, true, 55.5)",
      };
}
