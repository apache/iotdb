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
package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;

import java.io.IOException;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.*;

public class EngineNodeConstructor {

    private long jobId;

    public EngineNodeConstructor(long jobId) {
        this.jobId = jobId;
    }

    public Node construct(IExpression expression) throws IOException, FileNodeManagerException {
        if (expression.getType() == SERIES) {
            return new EngineLeafNode(generateSeriesReader((SingleSeriesExpression) expression));
        } else {
            Node leftChild;
            Node rightChild;
            if (expression.getType() == OR) {
                leftChild = this.construct(((IBinaryExpression) expression).getLeft());
                rightChild = this.construct(((IBinaryExpression) expression).getRight());
                return new OrNode(leftChild, rightChild);
            } else if (expression.getType() == AND) {
                leftChild = this.construct(((IBinaryExpression) expression).getLeft());
                rightChild = this.construct(((IBinaryExpression) expression).getRight());
                return new AndNode(leftChild, rightChild);
            } else {
                throw new UnSupportedDataTypeException(
                        "Unsupported QueryFilterType when construct OperatorNode: " + expression.getType());
            }
        }
    }

    private IReader generateSeriesReader(SingleSeriesExpression singleSeriesExpression)
            throws IOException, FileNodeManagerException {

        QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId,
                singleSeriesExpression.getSeriesPath());

        Filter filter = singleSeriesExpression.getFilter();

        PriorityMergeReader priorityReader = new PriorityMergeReader();

        // reader for all sequence data
        SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), filter);
        priorityReader.addReaderWithPriority(tsFilesReader, 1);

        // reader for all unSequence data
        PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
                .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), filter);
        priorityReader.addReaderWithPriority(unSeqMergeReader, 2);

        return priorityReader;
    }

}
