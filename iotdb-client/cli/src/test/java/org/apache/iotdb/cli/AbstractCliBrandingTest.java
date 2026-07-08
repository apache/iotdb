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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

public class AbstractCliBrandingTest {

  @Test
  public void echoStartingUsesBrandName() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream printer = new PrintStream(out, true, StandardCharsets.UTF_8.name());
    CliContext ctx = new CliContext(System.in, printer, System.err, ExitType.EXCEPTION);
    AbstractCli.echoStarting(ctx);
    assertTrue(out.toString().contains("Starting " + IoTDBConstant.BRAND_NAME + " Cli"));
  }
}
