/**
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
package org.apache.iotdb.cluster.service.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.service.nodetool.NodeTool.NodeToolCmd;
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;

@Command(name = "host", description = "Print all data partitions information which specific host belongs to")
public class Host extends NodeToolCmd {

  private static final int DEFAULT_PORT = -1;

  @Option(title = "ip", name = {"-i", "--ip"}, description = "Specify the host ip for accurate hosts information")
  private String ip = "127.0.0.1";

  @Option(title = "port", name = {"-p", "--port"}, description = "Specify the host port for accurate hosts information")
  private int port = DEFAULT_PORT;

  @Option(title = "sg_detail", name = {"-d", "--detail"}, description = "Show path of storage groups")
  private boolean detail = false;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    Map<String[], String[]> map;
    if (port == DEFAULT_PORT) {
      map = proxy.getDataPartitonOfNode(ip);
    } else {
      map = proxy.getDataPartitonOfNode(ip, port);
    }

    if (map == null) {
      System.out.println("Can't find the input IP.");
      return;
    }

    for (Entry<String[], String[]> entry : map.entrySet()) {
      StringBuilder builder = new StringBuilder();
      String[] ips = entry.getKey();
      String[] sgs = entry.getValue();
      builder.append('(');
      for (int i = 0; i < ips.length; i++) {
        builder.append(ips[i]).append(", ");
      }
      builder.delete(builder.length() - 2, builder.length());
      builder.append(')');

      builder.append("\t->\t");
      if (detail) {
        builder.append('(');
        for (int i = 0; i < sgs.length; i++) {
          builder.append(sgs[i]).append(", ");
        }
        if (sgs.length > 0) {
          builder.delete(builder.length() - 2, builder.length());
        }
        builder.append(')');
      } else {
        builder.append(sgs.length);
      }

      System.out.println(builder.toString());
    }
  }
}
