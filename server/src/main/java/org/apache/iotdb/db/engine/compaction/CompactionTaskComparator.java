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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;

import java.util.Comparator;

public class CompactionTaskComparator implements Comparator<AbstractCompactionTask> {
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public int compare(AbstractCompactionTask o1, AbstractCompactionTask o2) {
    if ((((o1 instanceof AbstractInnerSpaceCompactionTask)
                && !(o2 instanceof AbstractInnerSpaceCompactionTask))
            || ((o1 instanceof AbstractCrossSpaceCompactionTask)
                && !(o2 instanceof AbstractCrossSpaceCompactionTask)))
        && config.getCompactionPriority() != CompactionPriority.BALANCE) {
      // the two task is different type, and the compaction priority is not balance
      if (config.getCompactionPriority() == CompactionPriority.INNER_CROSS) {
        return o1 instanceof AbstractInnerSpaceCompactionTask ? 1 : -1;
      } else {
        return o1 instanceof AbstractCrossSpaceCompactionTask ? 1 : -1;
      }
    }
    if (o1 instanceof AbstractInnerSpaceCompactionTask) {
      return compareInnerSpaceCompactionTask(
          (AbstractInnerSpaceCompactionTask) o1, (AbstractInnerSpaceCompactionTask) o2);
    } else {
      return comparefCrossSpaceCompactionTask(
          (AbstractCrossSpaceCompactionTask) o1, (AbstractCrossSpaceCompactionTask) o2);
    }
  }

  private int compareInnerSpaceCompactionTask(
      AbstractInnerSpaceCompactionTask o1, AbstractInnerSpaceCompactionTask o2) {
    return -1;
  }

  private int comparefCrossSpaceCompactionTask(
      AbstractCrossSpaceCompactionTask o1, AbstractCrossSpaceCompactionTask o2) {
    return -1;
  }
}
