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

package org.apache.iotdb.db.queryengine.transformation.dag.util.visitor;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

public interface BinaryTransformerVisitor {
  ColumnTransformer visitInt(ColumnTransformer left, ColumnTransformer right);

  ColumnTransformer visitLong(ColumnTransformer left, ColumnTransformer right);

  ColumnTransformer visitFloat(ColumnTransformer left, ColumnTransformer right);

  ColumnTransformer visitDouble(ColumnTransformer left, ColumnTransformer right);

  ColumnTransformer visitDate(ColumnTransformer left, ColumnTransformer right);

  ColumnTransformer visitTimestamp(ColumnTransformer left, ColumnTransformer right);
}
