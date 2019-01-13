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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NodeTest {

    @Test
    public void testLeafNode() throws IOException {
        int index = 0;
        long[] timestamps = new long[] { 1, 2, 3, 4, 5, 6, 7 };
        FileSeriesReader seriesReader = new FakedFileSeriesReader(timestamps);
        Node leafNode = new LeafNode(seriesReader);
        while (leafNode.hasNext()) {
            Assert.assertEquals(timestamps[index++], leafNode.next());
        }
    }

    @Test
    public void testOrNode() throws IOException {
        long[] ret = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20 };
        long[] left = new long[] { 1, 3, 5, 7, 9, 10, 20 };
        long[] right = new long[] { 2, 3, 4, 5, 6, 7, 8 };
        testOr(ret, left, right);
        testOr(new long[] {}, new long[] {}, new long[] {});
        testOr(new long[] { 1 }, new long[] { 1 }, new long[] {});
        testOr(new long[] { 1 }, new long[] { 1 }, new long[] { 1 });
        testOr(new long[] { 1, 2 }, new long[] { 1 }, new long[] { 1, 2 });
        testOr(new long[] { 1, 2 }, new long[] { 1, 2 }, new long[] { 1, 2 });
        testOr(new long[] { 1, 2, 3 }, new long[] { 1, 2 }, new long[] { 1, 2, 3 });
    }

    private void testOr(long[] ret, long[] left, long[] right) throws IOException {
        int index = 0;
        Node orNode = new OrNode(new LeafNode(new FakedFileSeriesReader(left)),
                new LeafNode(new FakedFileSeriesReader(right)));
        while (orNode.hasNext()) {
            long value = orNode.next();
            Assert.assertEquals(ret[index++], value);
        }
        Assert.assertEquals(ret.length, index);
    }

    @Test
    public void testAndNode() throws IOException {
        testAnd(new long[] {}, new long[] { 1, 2, 3, 4 }, new long[] {});
        testAnd(new long[] {}, new long[] { 1, 2, 3, 4, 8 }, new long[] { 5, 6, 7 });
        testAnd(new long[] { 2 }, new long[] { 1, 2, 3, 4 }, new long[] { 2, 5, 6 });
        testAnd(new long[] { 1, 2, 3 }, new long[] { 1, 2, 3, 4 }, new long[] { 1, 2, 3 });
        testAnd(new long[] { 1, 2, 3, 9 }, new long[] { 1, 2, 3, 4, 9 }, new long[] { 1, 2, 3, 8, 9 });
    }

    private void testAnd(long[] ret, long[] left, long[] right) throws IOException {
        int index = 0;
        Node andNode = new AndNode(new LeafNode(new FakedFileSeriesReader(left)),
                new LeafNode(new FakedFileSeriesReader(right)));
        while (andNode.hasNext()) {
            long value = andNode.next();
            Assert.assertEquals(ret[index++], value);
        }
        Assert.assertEquals(ret.length, index);
    }

    private static class FakedFileSeriesReader extends FileSeriesReader {

        BatchData data;
        boolean hasCachedData;

        public FakedFileSeriesReader(long[] timestamps) {
            super(null, null);
            data = new BatchData(TSDataType.INT32, true);
            for (long time : timestamps) {
                data.putTime(time);
            }
            hasCachedData = true;
        }

        @Override
        public boolean hasNextBatch() {
            return hasCachedData;
        }

        @Override
        public BatchData nextBatch() {
            hasCachedData = false;
            return data;
        }

        @Override
        public BatchData currentBatch() {
            return data;
        }

        @Override
        protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {

        }

        @Override
        protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
            return false;
        }

        @Override
        public void close() {

        }
    }
}
