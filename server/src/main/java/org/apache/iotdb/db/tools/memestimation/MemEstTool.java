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

import com.google.common.collect.Lists;
import io.airlift.airline.Help;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.CommonUtils;

public class MemEstTool {

  public static void main(String... args) throws IOException {
    List<Class<? extends Runnable>> commands = Lists.newArrayList(
        Help.class,
        MemEstToolCmd.class
    );
    int status = CommonUtils.runCli(commands, args, "memory-tool", "Estimate memory for writing");

    FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir()));
    FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getWalDir()));
    for(int i=0; i < IoTDBDescriptor.getInstance().getConfig().getDataDirs().length; i++){
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(IoTDBDescriptor.getInstance().getConfig().getDataDirs()[i]));
    }

    System.exit(status);
  }
}
