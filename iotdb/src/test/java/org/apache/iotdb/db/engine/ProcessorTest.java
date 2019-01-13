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
package org.apache.iotdb.db.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.db.utils.EnvironmentUtils;

import java.io.IOException;

/**
 * @author liukun
 *
 */
public class ProcessorTest {

    class TestLRUProcessor extends Processor {

        public TestLRUProcessor(String nameSpacePath) {
            super(nameSpacePath);
        }

        @Override
        public boolean canBeClosed() {
            return false;
        }

        @Override
        public void close() throws ProcessorException {

        }

        @Override
        public boolean flush() throws IOException {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public long memoryUsage() {
            return 0;
        }

    }

    TestLRUProcessor processor1;
    TestLRUProcessor processor2;
    TestLRUProcessor processor3;

    @Before
    public void setUp() throws Exception {
        processor1 = new TestLRUProcessor("ns1");
        processor2 = new TestLRUProcessor("ns2");
        processor3 = new TestLRUProcessor("ns1");
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testEquals() {
        assertEquals(processor1, processor3);
        assertFalse(processor1.equals(processor2));
    }

    @Test
    public void testLockAndUnlock() throws InterruptedException {
        Thread thread = new Thread(new lockRunnable());

        thread.start();

        Thread.sleep(100);

        assertEquals(false, processor1.tryReadLock());
        assertEquals(false, processor1.tryLock(true));

        Thread.sleep(2000);

        assertEquals(true, processor1.tryLock(true));
        assertEquals(true, processor1.tryLock(false));

        processor1.readUnlock();
        processor1.writeUnlock();

        Thread thread2 = new Thread(new readLockRunable());
        thread2.start();
        Thread.sleep(100);

        assertEquals(false, processor1.tryWriteLock());
        assertEquals(true, processor1.tryReadLock());

        Thread.sleep(1500);
        assertEquals(false, processor1.tryWriteLock());
        processor1.readUnlock();
        assertEquals(true, processor1.tryWriteLock());
        processor1.writeUnlock();
    }

    class lockRunnable implements Runnable {

        @Override
        public void run() {
            processor1.lock(true);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            processor1.unlock(true);
        }
    }

    class readLockRunable implements Runnable {

        @Override
        public void run() {
            processor1.readLock();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            processor1.readUnlock();
        }

    }
}
