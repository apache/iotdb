/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.postback.conf;

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

/**
 * @author lta
 */
public class PostBackSenderConfig {

  public static final String CONFIG_NAME = "iotdb-postbackClient.properties";

  public String[] iotdbBufferwriteDirectory = IoTDBDescriptor.getInstance().getConfig()
      .getBufferWriteDirs();
  public String dataDirectory =
      new File(IoTDBDescriptor.getInstance().getConfig().dataDir).getAbsolutePath()
          + File.separator;
  public String uuidPath;
  public String lastFileInfo;
  public String[] snapshotPaths;
  public String schemaPath =
      new File(IoTDBDescriptor.getInstance().getConfig().metadataDir).getAbsolutePath()
          + File.separator + "mlog.txt";
  public String serverIp = "127.0.0.1";
  public int serverPort = 5555;
  public int clientPort = 6666;
  public int uploadCycleInSeconds = 10;
  public boolean isClearEnable = false;
}
