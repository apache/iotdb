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

import org.apache.iotdb.db.exception.WALOverSizedException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.exception.WALOverSizedException;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogWriterReaderTest {
    List<byte[]> logs = new ArrayList<>();
    List<PhysicalPlan> plans = new ArrayList<>();
    private static String filePath = "logtest.test";

    @Before
    public void prepare() throws WALOverSizedException {
        if (new File(filePath).exists())
            new File(filePath).delete();
        InsertPlan insertPlan1 = new InsertPlan(1, "d1", 10L, Arrays.asList("s1", "s2"), Arrays.asList("1", "2"));
        InsertPlan insertPlan2 = new InsertPlan(2, "d1", 10L, Arrays.asList("s1", "s2"), Arrays.asList("1", "2"));
        UpdatePlan updatePlan = new UpdatePlan(8L, 11L, "3", new Path("root.d1.s1"));
        DeletePlan deletePlan = new DeletePlan(10L, new Path("root.d1.s1"));
        plans.add(insertPlan1);
        plans.add(insertPlan2);
        plans.add(updatePlan);
        plans.add(deletePlan);
        for (PhysicalPlan plan : plans) {
            logs.add(PhysicalPlanLogTransfer.operatorToLog(plan));

        }
    }

    @Test
    public void testWriteAndRead() throws IOException {
        LogWriter writer = new LogWriter(filePath);
        writer.write(logs);
        try {
            writer.close();
            RAFLogReader reader = new RAFLogReader(new File(filePath));
            List<byte[]> res = new ArrayList<>();
            while (reader.hasNext()) {
                res.add(PhysicalPlanLogTransfer.operatorToLog(reader.next()));
            }
            for (int i = 0; i < logs.size(); i++) {
                assertArrayEquals(logs.get(i), res.get(i));
            }
            reader.close();
        } finally {
            new File(filePath).delete();
        }
    }
}
