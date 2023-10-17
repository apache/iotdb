/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
  private static final Logger logger = LoggerFactory.getLogger(PathCheckExample.class);

  public static void main(String[] args) {
    inputTest();
    check();
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
        logger.error("{} is not a legal path.", s);
      }
    }
  }

}
