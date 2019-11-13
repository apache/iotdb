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

package org.apache.iotdb.hadoop.fileSystem;


import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

public class HDFSConfUtil {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  public static Configuration setConf(Configuration conf) {
    try {
      conf.addResource(new File("/etc/hdfs1/conf/core-site.xml").toURI().toURL());
      conf.addResource(new File("/etc/hdfs1/conf/hdfs-site.xml").toURI().toURL());
      conf.addResource(new File("/etc/yarn1/conf/yarn-site.xml").toURI().toURL());
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

    // HA configuration
    String[] hdfsIps = tsFileConfig.getHdfsIp();
    if (hdfsIps.length > 1) {
      String dfsNameservices = tsFileConfig.getDfsNameServices();
      String[] dfsHaNamenodes = tsFileConfig.getDfsHaNamenodes();
      conf.set("dfs.nameservices", dfsNameservices);
      conf.set("dfs.ha.namenodes." + dfsNameservices, String.join(",", dfsHaNamenodes));
      for (int i = 0; i < dfsHaNamenodes.length; i++) {
        conf.set("dfs.namenode.rpc-address." + dfsNameservices + "." + dfsHaNamenodes[i].trim(),
            hdfsIps[i] + ":" + tsFileConfig.getHdfsPort());
      }
      boolean dfsHaAutomaticFailoverEnabled = tsFileConfig.isDfsHaAutomaticFailoverEnabled();
      conf.set("dfs.ha.automatic-failover.enabled", String.valueOf(dfsHaAutomaticFailoverEnabled));
      if (dfsHaAutomaticFailoverEnabled) {
        conf.set("dfs.client.failover.proxy.provider." + dfsNameservices,
            tsFileConfig.getDfsClientFailoverProxyProvider());
      }
    }

    // Kerberos configuration
    if (tsFileConfig.isUseKerberos()) {
      conf.set("hadoop.security.authorization", "true");
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("dfs.block.access.token.enable", "true");

//      conf.set("dfs.namenode.kerberos.principal", "nn/_" + tsFileConfig.getKerberosPrincipal());
//      conf.set("dfs.namenode.keytab.file", tsFileConfig.getKerberosKeytabFilePath());
//      conf.set("dfs.secondary.namenode.kerberos.principal", tsFileConfig.getKerberosPrincipal());
//      conf.set("dfs.secondary.namenode.keytab.file", tsFileConfig.getKerberosKeytabFilePath());
//      conf.set("dfs.datanode.kerberos.principal", "dn/" + tsFileConfig.getKerberosPrincipal());
//      conf.set("dfs.datanode.keytab.file", tsFileConfig.getKerberosKeytabFilePath());
//      conf.set("dfs.namenode.kerberos.internal.spnego.principal",
//          "HTTP/" + tsFileConfig.getKerberosKeytabFilePath());

      UserGroupInformation.setConfiguration(conf);
      try {
        UserGroupInformation.loginUserFromKeytab(tsFileConfig.getKerberosPrincipal(),
            tsFileConfig.getKerberosKeytabFilePath());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return conf;
  }
}