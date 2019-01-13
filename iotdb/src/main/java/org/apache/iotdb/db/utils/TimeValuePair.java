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
package org.apache.iotdb.db.utils;

import java.io.Serializable;

public class TimeValuePair implements Serializable {
    private long timestamp;
    private TsPrimitiveType value;

    public TimeValuePair(long timestamp, TsPrimitiveType value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TsPrimitiveType getValue() {
        return value;
    }

    public void setValue(TsPrimitiveType value) {
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(timestamp).append(" : ").append(getValue());
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof TimeValuePair) {
            return ((TimeValuePair) object).getTimestamp() == timestamp && ((TimeValuePair) object).getValue() != null
                    && ((TimeValuePair) object).getValue().equals(value);
        }
        return false;
    }

    public int getSize() {
        return 8 + 8 + value.getSize();
    }
}
