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

package org.apache.iotdb.db.calc_commons.execution.operator;

import org.apache.tsfile.read.common.block.column.LongColumn;

import java.util.Optional;

public class CommonOperatorUtils {
  public static final String CURRENT_DEVICE_INDEX_STRING = "CurrentDeviceIndex";
  public static final LongColumn TIME_COLUMN_TEMPLATE =
      new LongColumn(1, Optional.empty(), new long[] {0});
  public static final String CURRENT_USED_MEMORY = "CurrentUsedMemory";
  public static final String MAX_USED_MEMORY = "MaxUsedMemory";
  public static final String MAX_RESERVED_MEMORY = "MaxReservedMemory";
}
