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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Counter<T> implements Externalizable {

  protected ListNode2<StreamSummary<T>.Bucket> bucketNode;

  protected T item;
  protected long count;
  protected long error;

  /** For de-serialization */
  public Counter() {}

  public Counter(ListNode2<StreamSummary<T>.Bucket> bucket, T item) {
    this.bucketNode = bucket;
    this.count = 0;
    this.error = 0;
    this.item = item;
  }

  public T getItem() {
    return item;
  }

  public long getCount() {
    return count;
  }

  public long getError() {
    return error;
  }

  @Override
  public String toString() {
    return item + ":" + count + ':' + error;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    item = (T) in.readObject();
    count = in.readLong();
    error = in.readLong();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(item);
    out.writeLong(count);
    out.writeLong(error);
  }
}
