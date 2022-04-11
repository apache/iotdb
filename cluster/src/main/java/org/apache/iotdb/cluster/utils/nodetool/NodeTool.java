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

import org.apache.iotdb.cluster.utils.nodetool.function.Header;
import org.apache.iotdb.cluster.utils.nodetool.function.LogView;
import org.apache.iotdb.cluster.utils.nodetool.function.Migration;
import org.apache.iotdb.cluster.utils.nodetool.function.Partition;
import org.apache.iotdb.cluster.utils.nodetool.function.Ring;
import org.apache.iotdb.cluster.utils.nodetool.function.Slot;
import org.apache.iotdb.cluster.utils.nodetool.function.Status;
import org.apache.iotdb.db.utils.CommonUtils;

import com.google.common.collect.Lists;
import io.airlift.airline.Help;

import java.util.List;

public class NodeTool {

  public static void main(String... args) {
    List<Class<? extends Runnable>> commands =
        Lists.newArrayList(
            Help.class,
            Ring.class,
            Partition.class,
            Slot.class,
            Status.class,
            LogView.class,
            Migration.class,
            Header.class);

    int status = CommonUtils.runCli(commands, args, "nodetool", "Manage your IoTDB cluster");
    System.exit(status);
  }
}
