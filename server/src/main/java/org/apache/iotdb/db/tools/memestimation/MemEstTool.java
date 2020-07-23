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
package org.apache.iotdb.db.tools.memestimation;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;

import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

public class MemEstTool {

  public static void main(String... args) throws IOException {
    List<Class<? extends Runnable>> commands = Lists.newArrayList(
        Help.class,
        MemEstToolCmd.class
    );
    Cli.CliBuilder<Runnable> builder = Cli.builder("memory-tool");

    builder.withDescription("Estimate memory for writing")
        .withDefaultCommand(Help.class)
        .withCommands(commands);

    Cli<Runnable> parser = builder.build();

    int status = 0;
    try {
      Runnable parse = parser.parse(args);
      parse.run();
    } catch (IllegalArgumentException |
        IllegalStateException |
        ParseArgumentsMissingException |
        ParseArgumentsUnexpectedException |
        ParseOptionConversionException |
        ParseOptionMissingException |
        ParseOptionMissingValueException |
        ParseCommandMissingException |
        ParseCommandUnrecognizedException e) {
      badUse(e);
      status = 1;
    } catch (Exception e) {
      err(Throwables.getRootCause(e));
      status = 2;
    }

    FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir()));
    FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getWalDir()));
    for(int i=0; i < IoTDBDescriptor.getInstance().getConfig().getDataDirs().length; i++){
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getDataDirs()[i]));
    }

    System.exit(status);
  }

  private static void badUse(Exception e) {
    System.out.println("memory-tool: " + e.getMessage());
    System.out.println("See 'memory-tool help' or 'memory-tool help <command>'.");
  }

  private static void err(Throwable e) {
    System.err.println("error: " + e.getMessage());
    System.err.println("-- StackTrace --");
    System.err.println(Throwables.getStackTraceAsString(e));
  }
}
