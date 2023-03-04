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
package org.apache.iotdb.cluster.utils.nodetool;

import java.io.PrintStream;

@SuppressWarnings("java:S106") // for console outputs
public class Printer {

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final PrintStream ERR_PRINTER = new PrintStream(System.err);

  private Printer() {}

  public static void msgPrintln(String s) {
    SCREEN_PRINTER.println(s);
  }

  public static void errPrintln(String s) {
    ERR_PRINTER.println(s);
  }
}
