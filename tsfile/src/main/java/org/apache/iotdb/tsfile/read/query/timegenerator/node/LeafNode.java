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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

import java.io.IOException;

public class LeafNode implements Node {

    private FileSeriesReader reader;

    private BatchData data = null;

    private boolean gotData = false;

    public LeafNode(FileSeriesReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {

        if (gotData) {
            data.next();
            gotData = false;
        }

        if (data == null || !data.hasNext()) {
            if (reader.hasNextBatch())
                data = reader.nextBatch();
            else
                return false;
        }

        return data.hasNext();
    }

    @Override
    public long next() {
        long time = data.currentTime();
        gotData = true;
        return time;
    }

    public boolean currentTimeIs(long time) {
        if (!reader.currentBatch().hasNext())
            return false;
        return reader.currentBatch().currentTime() == time;
    }

    public Object currentValue(long time) {
        if (data.currentTime() == time)
            return data.currentValue();
        return null;
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }

}
