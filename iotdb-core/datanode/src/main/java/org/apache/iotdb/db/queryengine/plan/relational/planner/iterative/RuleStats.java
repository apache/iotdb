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
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative;

import java.util.concurrent.atomic.AtomicLong;

public class RuleStats {
  private final AtomicLong invocations = new AtomicLong();
  private final AtomicLong hits = new AtomicLong();
  // private final TimeDistribution time = new TimeDistribution(TimeUnit.MICROSECONDS);
  private final AtomicLong failures = new AtomicLong();

  public void record(long nanos, boolean match) {
    if (match) {
      hits.incrementAndGet();
    }

    invocations.incrementAndGet();
    // time.add(nanos);
  }

  public void recordFailure() {
    failures.incrementAndGet();
  }

  public long getInvocations() {
    return invocations.get();
  }

  public long getHits() {
    return hits.get();
  }

  /*public TimeDistribution getTime()
  {
      return time;
  }*/

  public long getFailures() {
    return failures.get();
  }
}
