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
 *
 */
package org.apache.iotdb.db.newsync.receiver.collector;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;

/**
 * scan sync receiver folder and load pipeData into IoTDB
 */
public class Collector {

    private static final Logger logger = LoggerFactory.getLogger(Collector.class);
    // TODO: multi thread for multi pipe
    private ExecutorService executorService;

    public Collector(){
        this.executorService =
                IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
    }

    public static void main(String[] args) throws IOException, IllegalPathException {
        File f1 = new File("testtt");
        File f2 = new File("testtt");
        TsFilePipeData pipeData1 = new TsFilePipeData("1",1);
        TsFilePipeData pipeData2 = new TsFilePipeData("2",2);
        DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(f2));
        pipeData1.serialize(outputStream);
        outputStream.flush();
        DataInputStream inputStream = new DataInputStream(new FileInputStream(f1));
        System.out.println(TsFilePipeData.deserialize(inputStream).toString());
        pipeData2.serialize(outputStream);
        outputStream.flush();
        System.out.println(TsFilePipeData.deserialize(inputStream).toString());
        Files.deleteIfExists(f1.toPath());
    }
}
