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
package org.apache.iotdb.tsfile.read.filter.basic;

import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

import java.io.Serializable;

/**
 * Definition for unary filter operations
 *
 * @param <T>
 *            comparable data type
 * @author CGF
 */
public abstract class UnaryFilter<T extends Comparable<T>> implements Filter, Serializable {

    private static final long serialVersionUID = 1431606024929453556L;
    protected final T value;

    protected FilterType filterType;

    protected UnaryFilter(T value, FilterType filterType) {
        this.value = value;
        this.filterType = filterType;
    }

    public T getValue() {
        return value;
    }

    public FilterType getFilterType() {
        return filterType;
    }

    @Override
    public abstract String toString();
}
