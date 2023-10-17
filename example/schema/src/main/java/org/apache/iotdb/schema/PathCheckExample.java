package org.apache.iotdb.schema;

import org.apache.iotdb.tsfile.exception.PathParseException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Before creating paths to IoTDB, you should better check if the path is correct or not to avoid
 * some errors.
 *
 * <p>This example will check the paths in the inputList and output the erroneous paths to the
 * errorList.
 */
public class PathCheckExample {
  private static final List<String> inputList = new ArrayList<>();
  private static final List<String> errorList = new ArrayList<>();

  private static final Logger logger = LoggerFactory.getLogger(PathCheckExample.class);

  public static void main(String[] args) {
    inputTest();
    check();
    output();
  }

  private static void inputTest() {
    inputList.add("root.test.d1.s1");
    inputList.add("root.b+.d1.s2");
    inputList.add("root.test.1.s3");
    inputList.add("root.test.d-j.s4");
    inputList.add("root.test.'8`7'.s5");
    inputList.add("root.test.`1`.s6");
    inputList.add("root.test.\"d+b\".s7");
  }

  private static void check() {
    for (String s : inputList) {
      try {
        PathNodesGenerator.checkPath(s);
      } catch (PathParseException e) {
        errorList.add(s);
      }
    }
  }

  private static void output() {
    if (errorList.isEmpty()) {
      logger.info("All paths are correct.");
    } else {
      for (String s : errorList) {
        logger.error("{} is not a legal path.", s);
      }
    }
  }
}
