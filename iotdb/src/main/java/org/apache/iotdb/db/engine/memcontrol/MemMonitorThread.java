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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemMonitorThread extends Thread {

    private static Logger logger = LoggerFactory.getLogger(MemMonitorThread.class);

    private long checkInterval = 1000; // in ms

    private Policy safePolicy;
    private Policy warningPolicy;
    private Policy dangerousPolicy;

    public MemMonitorThread(IoTDBConfig config) {
        this.setName(ThreadName.MEMORY_MONITOR.getName());
        long checkInterval = config.memMonitorInterval;
        this.checkInterval = checkInterval > 0 ? checkInterval : this.checkInterval;
        if (config.enableSmallFlush)
            this.safePolicy = new FlushPartialPolicy();
        else
            this.safePolicy = new NoActPolicy();
        this.warningPolicy = new ForceFLushAllPolicy();
        this.dangerousPolicy = new ForceFLushAllPolicy();
    }

    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void run() {
        logger.info("{} started", this.getClass().getSimpleName());
        super.run();
        while (true) {
            if (this.isInterrupted()) {
                logger.info("{} exiting...", this.getClass().getSimpleName());
                return;
            }
            BasicMemController.UsageLevel level = BasicMemController.getInstance().getCurrLevel();
            switch (level) {
            case WARNING:
                warningPolicy.execute();
                break;
            case DANGEROUS:
                dangerousPolicy.execute();
                break;
            case SAFE:
                safePolicy.execute();
                break;
            default:
                logger.error("Unknown usage level : {}", level);
            }
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.info("{} exiting...", this.getClass().getSimpleName());
                return;
            }
        }
    }
}
