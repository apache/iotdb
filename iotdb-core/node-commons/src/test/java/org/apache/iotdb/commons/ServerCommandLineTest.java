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
package org.apache.iotdb.commons;

import org.apache.iotdb.commons.exception.IoTDBException;

import junit.framework.TestCase;
import org.junit.Assert;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ServerCommandLineTest extends TestCase {

  /**
   * In this test, the commandline is called without any args. In this case the usage should be
   * output and nothing should be done.
   */
  public void testNoArgs() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
          }
        };
    int returnCode = commandLine.run(new String[0]);

    Assert.assertEquals(1, returnCode);
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.contains("Missing required option"));
    // No callbacks should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with an invalid arg. In this case the usage should be
   * output and nothing should be done.
   */
  public void testInvalidArgs() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
          }
        };
    int returnCode = commandLine.run(new String[] {"-z"});

    Assert.assertEquals(1, returnCode);
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.contains("Unrecognized option"));
    // No callbacks should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with the start option. The start method should be
   * called.
   */
  public void testStartArg() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
          }
        };
    int returnCode = commandLine.run(new String[] {"-s"});

    Assert.assertEquals(0, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.isEmpty());
    // Only the start method should have been called.
    Assert.assertTrue(startCalled.get());
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with the remove option, but without an additional
   * attribute for providing the node id. The stop method should be called and "null" should be
   * provided as node id.
   */
  public void testRemoveArg() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    AtomicLong stopNodeId = new AtomicLong(-1);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
            if (nodeId != null) {
              stopNodeId.set(nodeId);
            }
          }
        };
    int returnCode = commandLine.run(new String[] {"-r"});

    Assert.assertEquals(0, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.isEmpty());
    // Only the start method should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertTrue(stopCalled.get());
    Assert.assertEquals(-1, stopNodeId.get());
  }

  /**
   * In this test, the commandline is called with the remove option, with an additional attribute
   * for providing the node id. The stop method should be called and the id should be passed to the
   * remove callback.
   */
  public void testRemoveWithNodeIdArg() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    AtomicLong stopNodeId = new AtomicLong(-1);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
            if (nodeId != null) {
              stopNodeId.set(nodeId);
            }
          }
        };
    int returnCode = commandLine.run(new String[] {"-r", "42"});

    Assert.assertEquals(0, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.isEmpty());
    // Only the start method should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertTrue(stopCalled.get());
    Assert.assertEquals(42L, stopNodeId.get());
  }

  /**
   * In this test, the commandline is called with the remove option, with an additional attribute
   * for providing the node id. However, the attribute is not an integer value, therefore an error
   * should be thrown.
   */
  public void testRemoveWithInvalidNodeIdArg() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    AtomicLong stopNodeId = new AtomicLong(-1);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
            if (nodeId != null) {
              stopNodeId.set(nodeId);
            }
          }
        };
    int returnCode = commandLine.run(new String[] {"-r", "text"});

    Assert.assertEquals(1, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.contains("For input string"));
    // No callbacks should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with the both the start and stop option. This should
   * result in an error report and no callback should be called.
   */
  public void testCallWithMultipleActions() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    AtomicLong stopNodeId = new AtomicLong(-1);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
            if (nodeId != null) {
              stopNodeId.set(nodeId);
            }
          }
        };
    int returnCode = commandLine.run(new String[] {"-r", "42", "-s"});

    Assert.assertEquals(1, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(
        consoleOutput.contains("but an option from this group has already been selected"));
    // No callbacks should have been called.
    Assert.assertFalse(startCalled.get());
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with the start option. The start method should be
   * called, however there an exception is thrown, which should be caught.
   */
  public void testStartWithErrorArg() {
    AtomicBoolean stopCalled = new AtomicBoolean(false);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() throws IoTDBException {
            throw new IoTDBException("Error", 23);
          }

          @Override
          protected void remove(Long nodeId) {
            stopCalled.set(true);
          }
        };
    int returnCode = commandLine.run(new String[] {"-s"});

    Assert.assertEquals(23, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.contains("An error occurred while running the command"));
    // Only the start method should have been called.
    Assert.assertFalse(stopCalled.get());
  }

  /**
   * In this test, the commandline is called with the remove option, with an additional attribute
   * for providing the node id. The stop method should be called and the id should be passed to the
   * remove callback, however there an exception is thrown, which should be caught.
   */
  public void testRemoveWithNodeIdWithErrorArg() {
    AtomicBoolean startCalled = new AtomicBoolean(false);
    StringWriter out = new StringWriter();
    PrintWriter writer = new PrintWriter(out);
    ServerCommandLine commandLine =
        new ServerCommandLine("test-cli", writer) {
          @Override
          protected void start() {
            startCalled.set(true);
          }

          @Override
          protected void remove(Long nodeId) throws IoTDBException {
            throw new IoTDBException("Error", 23);
          }
        };
    int returnCode = commandLine.run(new String[] {"-r", "42"});

    Assert.assertEquals(23, returnCode);
    // Nothing should have been output on the console.
    String consoleOutput = out.toString();
    Assert.assertTrue(consoleOutput.contains("An error occurred while running the command"));
    // Only the start method should have been called.
    Assert.assertFalse(startCalled.get());
  }
}
