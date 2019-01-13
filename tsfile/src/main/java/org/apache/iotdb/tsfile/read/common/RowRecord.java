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
package org.apache.iotdb.tsfile.read.common;

import java.util.ArrayList;
import java.util.List;

public class RowRecord {
    private long timestamp;
    private List<Field> fields;

    public RowRecord(long timestamp) {
        this.timestamp = timestamp;
        this.fields = new ArrayList<>();
    }

    public long getTime() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void addField(Field f) {
        this.fields.add(f);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);
        for (Field f : fields) {
            sb.append("\t");
            sb.append(f);
        }
        return sb.toString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<Field> getFields() {
        return fields;
    }
}
