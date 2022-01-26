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
import org.apache.iotdb.db.newsync.utils.SyncPathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * scan sync receiver folder and load pipeData into IoTDB
 */
public class Collector {

    private static final Logger logger = LoggerFactory.getLogger(Collector.class);
    // TODO: multi thread for multi pipe
    private ExecutorService executorService;
    private ScanTask task;

    public Collector(){
        this.executorService =
                IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
        this.task = new ScanTask();
    }

    public void startCollect(){
        task.start();
        executorService =
                IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
        executorService.submit(task);
    }

    public void stopCollect(){
        task.stop();
        executorService.shutdown();
    }

    public void startPipe(String pipeName, String remoteIp, long createTime){
        task.addScanDir(SyncPathUtil.getReceiverPipeLogDir(pipeName,remoteIp,createTime));
    }

    public void stopPipe(String pipeName, String remoteIp,long createTime){
        task.removeScanDir(SyncPathUtil.getReceiverPipeLogDir(pipeName,remoteIp,createTime));
    }

//    public static void main(String[] args) throws IOException, IllegalPathException {
//        File f1 = new File("testtt");
//        File f2 = new File("testtt");
//        TsFilePipeData pipeData1 = new TsFilePipeData("1",1);
//        TsFilePipeData pipeData2 = new TsFilePipeData("2",2);
//        DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(f2));
//        pipeData1.serialize(outputStream);
//        outputStream.flush();
//        DataInputStream inputStream = new DataInputStream(new FileInputStream(f1));
//        System.out.println(TsFilePipeData.deserialize(inputStream).toString());
//        pipeData2.serialize(outputStream);
//        outputStream.flush();
//        System.out.println(TsFilePipeData.deserialize(inputStream).toString());
//        Files.deleteIfExists(f1.toPath());
//        inputStream.close();
//    }

    private class ScanTask implements Runnable{
        private Set<String> scanPathSet;
        private volatile boolean stopped;

        private ScanTask(){
            scanPathSet = new HashSet<>();
            stopped = false;
        }

        private void addScanDir(String dirPath){
            scanPathSet.add(dirPath);
        }

        private void removeScanDir(String dirPath){
            scanPathSet.remove(dirPath);
        }

        private void start(){
            this.stopped = false;
        }

        private void stop(){
            this.stopped = true;
        }

        @Override
        public void run() {
            try {
                while (!stopped) {

                }
            } catch (Exception e) {
            }
        }
    }
}
