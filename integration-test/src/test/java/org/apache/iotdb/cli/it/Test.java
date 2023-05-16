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

package org.apache.iotdb.cli.it;

import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Test {
  public static void main(String[] args) {
    List<Binary> binaryList = new ArrayList<>();
    Binary s = new Binary("abv");
    Binary d = new Binary("功能");
    binaryList.add(new Binary("功能码_DOUBLE"));
    binaryList.add(new Binary("`出水NH4-N_DOUBLE`"));
    binaryList.sort(Comparator.naturalOrder());
    List<String> stringList = new ArrayList<>();
    stringList.add("功能码_DOUBLE");
    stringList.add("`出水NH4-N_DOUBLE`");
    stringList.sort(Comparator.naturalOrder());
    System.out.println(stringList);
    System.out.println(binaryList);
  }
}
