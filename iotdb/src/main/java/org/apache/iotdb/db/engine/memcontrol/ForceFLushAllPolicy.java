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
package org.apache.iotdb.db.engine.memcontrol;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceFLushAllPolicy implements Policy {
    private static final Logger logger = LoggerFactory.getLogger(ForceFLushAllPolicy.class);
    private Thread workerThread;

    @Override
    public void execute() {
        logger.info("Memory reaches {}, current memory size is {}, JVM memory is {}, flushing.",
                BasicMemController.getInstance().getCurrLevel(),
                MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()),
                MemUtils.bytesCntToStr(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        // use a thread to avoid blocking
        if (workerThread == null) {
            workerThread = createWorkerThread();
            workerThread.start();
        } else {
            if (workerThread.isAlive()) {
                logger.info("Last flush is ongoing...");
            } else {
                workerThread = createWorkerThread();
                workerThread.start();
            }
        }
    }

    private Thread createWorkerThread() {
        return new Thread(() -> {
            FileNodeManager.getInstance().forceFlush(BasicMemController.UsageLevel.DANGEROUS);
            System.gc();
        }, ThreadName.FORCE_FLUSH_ALL_POLICY.getName());
    }
}
