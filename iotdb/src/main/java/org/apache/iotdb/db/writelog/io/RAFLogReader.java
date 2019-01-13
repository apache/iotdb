/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

public class RAFLogReader implements ILogReader {

    private static final Logger logger = LoggerFactory.getLogger(RAFLogReader.class);
    private RandomAccessFile logRAF;
    private String filepath;
    private int bufferSize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[bufferSize];
    private CRC32 checkSummer = new CRC32();
    private PhysicalPlan planBuffer = null;

    public RAFLogReader() {

    }

    public RAFLogReader(File logFile) throws FileNotFoundException {
        open(logFile);
    }

    @Override
    public boolean hasNext() {
        if (planBuffer != null)
            return true;
        try {
            if (logRAF.getFilePointer() + 12 > logRAF.length()) {
                return false;
            }
        } catch (IOException e) {
            logger.error("Cannot read from log file {}, because {}", filepath, e.getMessage());
            return false;
        }
        try {
            int logSize = logRAF.readInt();
            if (logSize > bufferSize) {
                bufferSize = logSize;
                buffer = new byte[bufferSize];
            }
            long checkSum = logRAF.readLong();
            logRAF.read(buffer, 0, logSize);
            checkSummer.reset();
            checkSummer.update(buffer, 0, logSize);
            if (checkSummer.getValue() != checkSum)
                return false;
            PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
            planBuffer = plan;
            return true;
        } catch (IOException e) {
            logger.error("Cannot read log file {}, because {}", filepath, e.getMessage());
            return false;
        }
    }

    @Override
    public PhysicalPlan next() {
        PhysicalPlan ret = planBuffer;
        planBuffer = null;
        return ret;
    }

    @Override
    public void close() {
        if (logRAF != null) {
            try {
                logRAF.close();
            } catch (IOException e) {
                logger.error("Cannot close log file {}", filepath);
            }
        }
    }

    @Override
    public void open(File logFile) throws FileNotFoundException {
        logRAF = new RandomAccessFile(logFile, "r");
        this.filepath = logFile.getPath();
    }
}
