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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;

public class TimeValuePairInMemTable extends TimeValuePair implements Comparable<TimeValuePairInMemTable> {

    public TimeValuePairInMemTable(long timestamp, TsPrimitiveType value) {
        super(timestamp, value);
    }

    @Override
    public int compareTo(TimeValuePairInMemTable o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof TimeValuePairInMemTable))
            return false;
        TimeValuePairInMemTable o = (TimeValuePairInMemTable) object;
        return o.getTimestamp() == this.getTimestamp();
    }
}