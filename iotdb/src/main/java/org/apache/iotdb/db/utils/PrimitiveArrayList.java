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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class PrimitiveArrayList {

    private static final int MAX_SIZE_OF_ONE_ARRAY = 512;
    private static final int INITIAL_SIZE = 1;

    private Class clazz;
    private List<Object> values;
    private List<long[]> timestamps;

    private int length; // Total size of all objects of current ArrayList
    private int currentIndex; // current index of array
    private int currentArrayIndex; // current index of element in current array
    private int currentArraySize; // size of current array

    public PrimitiveArrayList(Class clazz) {
        this.clazz = clazz;
        values = new ArrayList<>();
        timestamps = new ArrayList<>();
        values.add(Array.newInstance(clazz, INITIAL_SIZE));
        timestamps.add(new long[INITIAL_SIZE]);
        length = 0;

        currentIndex = 0;
        currentArraySize = INITIAL_SIZE;
        currentArrayIndex = -1;
    }

    private void capacity(int aimSize) {
        if (currentArraySize < aimSize) {
            if (currentArraySize < MAX_SIZE_OF_ONE_ARRAY) {
                // expand current Array
                int newCapacity = Math.min(MAX_SIZE_OF_ONE_ARRAY, currentArraySize * 2);
                values.set(currentIndex, expandArray(values.get(currentIndex), currentArraySize, newCapacity));
                timestamps.set(currentIndex,
                        (long[]) expandArray(timestamps.get(currentIndex), currentArraySize, newCapacity));
                currentArraySize = newCapacity;
            } else {
                // add a new Array to the list;
                values.add(Array.newInstance(clazz, INITIAL_SIZE));
                timestamps.add(new long[INITIAL_SIZE]);
                currentIndex++;
                currentArraySize = INITIAL_SIZE;
                currentArrayIndex = -1;
            }
        }
    }

    private Object expandArray(Object array, int preLentgh, int aimLength) {
        Class clazz = array.getClass().getComponentType();
        Object newArray = Array.newInstance(clazz, aimLength);
        System.arraycopy(array, 0, newArray, 0, preLentgh);
        return newArray;
    }

    public void putTimestamp(long timestamp, Object value) {
        capacity(currentArrayIndex + 1 + 1);
        currentArrayIndex++;
        timestamps.get(currentIndex)[currentArrayIndex] = timestamp;
        Array.set(values.get(currentIndex), currentArrayIndex, value);
        length++;
    }

    public long getTimestamp(int index) {
        checkIndex(index);
        return timestamps.get(index / MAX_SIZE_OF_ONE_ARRAY)[index % MAX_SIZE_OF_ONE_ARRAY];
    }

    public Object getValue(int index) {
        checkIndex(index);
        return Array.get(values.get(index / MAX_SIZE_OF_ONE_ARRAY), index % MAX_SIZE_OF_ONE_ARRAY);
    }

    private void checkIndex(int index) {
        if (index < 0) {
            throw new NegativeArraySizeException("negetive array index:" + index);
        }
        if (index >= length) {
            throw new ArrayIndexOutOfBoundsException("index: " + index);
        }
    }

    public int size() {
        return length;
    }

    public PrimitiveArrayList clone() {
        PrimitiveArrayList cloneList = new PrimitiveArrayList(clazz);
        cloneList.values.clear();
        cloneList.timestamps.clear();
        for (Object valueArray : values) {
            cloneList.values.add(cloneArray(valueArray, clazz));
        }
        for (Object timestampArray : timestamps) {
            cloneList.timestamps.add((long[]) cloneArray(timestampArray, long.class));
        }
        cloneList.length = length;
        cloneList.currentIndex = currentIndex;
        cloneList.currentArrayIndex = currentArrayIndex;
        cloneList.currentArraySize = currentArraySize;
        return cloneList;
    }

    private Object cloneArray(Object array, Class clazz) {
        Object cloneArray = Array.newInstance(clazz, Array.getLength(array));
        System.arraycopy(array, 0, cloneArray, 0, Array.getLength(array));
        return cloneArray;
    }
}
