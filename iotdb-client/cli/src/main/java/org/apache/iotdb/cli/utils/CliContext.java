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

package org.apache.iotdb.cli.utils;

import org.apache.iotdb.cli.type.ExitType;

import org.jline.reader.LineReader;

import java.io.InputStream;
import java.io.PrintStream;

public class CliContext {

  private final InputStream in;
  private final PrintStream out;
  private final PrintStream err;

  private final IoTPrinter printer;

  private final ExitType exitType;

  private final boolean disableCliHistory;

  private LineReader lineReader;

  public CliContext(InputStream in, PrintStream out, PrintStream err, ExitType exitType) {
    this(in, out, err, exitType, false);
  }

  public CliContext(
      InputStream in,
      PrintStream out,
      PrintStream err,
      ExitType exitType,
      boolean disableCliHistory) {
    this.in = in;
    this.out = out;
    this.err = err;
    this.exitType = exitType;
    this.printer = new IoTPrinter(out);
    this.disableCliHistory = disableCliHistory;
  }

  public InputStream getIn() {
    return in;
  }

  public PrintStream getOut() {
    return out;
  }

  public PrintStream getErr() {
    return err;
  }

  public IoTPrinter getPrinter() {
    return printer;
  }

  public ExitType getExitType() {
    return exitType;
  }

  public boolean isDisableCliHistory() {
    return disableCliHistory;
  }

  public LineReader getLineReader() {
    return lineReader;
  }

  public void setLineReader(LineReader lineReader) {
    this.lineReader = lineReader;
  }

  public void exit(int exitCode) {
    if (exitType == ExitType.SYSTEM_EXIT) {
      System.exit(exitCode);
    } else {
      throw new RuntimeException("Exiting with code " + exitCode);
    }
  }
}
