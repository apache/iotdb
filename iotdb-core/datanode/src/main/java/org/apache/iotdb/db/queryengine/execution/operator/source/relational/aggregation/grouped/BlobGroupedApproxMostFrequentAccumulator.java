/*
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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import com.google.gson.Gson;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class BlobGroupedApproxMostFrequentAccumulator
    extends BinaryGroupedApproxMostFrequentAccumulator {
  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    Map<Binary, Long> buckets = state.getSpaceSavings().get(groupId).getBuckets();
    Map<String, Long> formatedBuckets =
        buckets.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> BytesUtils.parseBlobByteArrayToString(entry.getKey().getValues()),
                    Map.Entry::getValue));
    columnBuilder.writeBinary(
        new Binary(new Gson().toJson(formatedBuckets), StandardCharsets.UTF_8));
  }
}
