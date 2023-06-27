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
package org.apache.iotdb.db.tools.settle;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSettleReq;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.util.Arrays;

public class TsFileSettleByCompactionTool {

  private static final String HOST_ARGS = "h";
  private static final String HOST_NAME = "host";

  private static final String PORT_ARGS = "p";
  private static final String PORT_NAME = "port";

  private static final String FILE_PATH_ARGS = "f";
  private static final String FILE_PATH_NAME = "file paths";

  private static final String DEFAULT_HOST_VALUE = "127.0.0.1";
  private static final String DEFAULT_PORT_VALUE = "10730";

  public static void main(String[] args) throws TException {
    String[] filePaths;

    Options commandLineOptions = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(commandLineOptions, args);
    } catch (ParseException e) {
      System.out.println("Parse command line args failed: " + e.getMessage());
      return;
    }

    String hostValue = getArgOrDefault(commandLine, HOST_ARGS, DEFAULT_HOST_VALUE);
    String portValue = getArgOrDefault(commandLine, PORT_ARGS, DEFAULT_PORT_VALUE);
    int port = Integer.parseInt(portValue);
    filePaths = commandLine.getOptionValues(FILE_PATH_ARGS);

    TTransport transport = new TFramedTransport(new TSocket(hostValue, port));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    IDataNodeRPCService.Client.Factory clientFactory = new IDataNodeRPCService.Client.Factory();
    IDataNodeRPCService.Client client = clientFactory.getClient(protocol);

    TSettleReq tSettleReq = new TSettleReq();
    tSettleReq.setPaths(Arrays.asList(filePaths));
    TSStatus result = client.settle(tSettleReq);
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      System.out.println("Add Settle Compaction Task Successfully");
    } else {
      System.out.println("Add settle compaction task failed with status code: " + result);
    }
  }

  private static Options createOptions() {
    Options options = new Options();
    Option host =
        Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (optional, default 127.0.0.1")
            .build();
    options.addOption(host);

    Option port =
        Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (optional, default 10730)")
            .build();
    options.addOption(port);

    Option filePaths =
        Option.builder(FILE_PATH_ARGS)
            .argName(FILE_PATH_NAME)
            .hasArgs()
            .valueSeparator(' ')
            .desc("File Paths (required)")
            .required()
            .build();
    options.addOption(filePaths);
    return options;
  }

  private static String getArgOrDefault(CommandLine commandLine, String arg, String defaultValue) {
    String value = commandLine.getOptionValue(arg);
    return value == null ? defaultValue : value;
  }
}
