/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.rescon.quotas;

public abstract class RateLimiter {

  // Timeunit factor for translating to ms.
  private long tunit = 1000;
  // The max value available resource units can be refilled to.
  private long limit = Long.MAX_VALUE;
  // Currently available resource units
  private long avail = Long.MAX_VALUE;

  /**
   * Set the RateLimiter max available resources and refill period.
   *
   * @param limit The max value available resource units can be refilled to.
   */
  public synchronized void set(final long limit, final long tunit) {
    this.tunit = tunit;
    this.limit = limit;
    this.avail = limit;
  }

  /**
   * Are there enough available resources to allow execution?
   *
   * @param amount the number of required resources, a non-negative number
   * @return true if there are enough available resources, otherwise false
   */
  public synchronized boolean canExecute(final long amount) {
    if (limit == Long.MAX_VALUE) {
      return true;
    }
    long refillAmount = refill(limit);
    if (refillAmount == 0 && avail < amount) {
      return false;
    }
    // check for positive overflow
    if (avail <= Long.MAX_VALUE - refillAmount) {
      avail = Math.max(0, Math.min(avail + refillAmount, limit));
    } else {
      avail = Math.max(0, limit);
    }
    if (avail >= amount) {
      return true;
    }
    return false;
  }

  /**
   * Refill the available units w.r.t the elapsed time.
   *
   * @param limit Maximum available resource units that can be refilled to.
   * @return how many resource units may be refilled ?
   */
  abstract long refill(long limit);

  /**
   * Time in milliseconds to wait for before requesting to consume 'amount' resource.
   *
   * @param limit Maximum available resource units that can be refilled to.
   * @param available Currently available resource units
   * @param amount Resources for which time interval to calculate for
   * @return estimate of the ms required to wait before being able to provide 'amount' resources.
   */
  abstract long getWaitInterval(long limit, long available, long amount);

  public synchronized long getTimeUnitInMillis() {
    return tunit;
  }

  public synchronized long getLimit() {
    return limit;
  }

  public synchronized long getAvailable() {
    return avail;
  }

  /** Returns estimate of the ms required to wait before being able to provide 1 resource. */
  public long waitInterval() {
    return waitInterval(1);
  }

  /**
   * Returns estimate of the ms required to wait before being able to provide "amount" resources.
   */
  public synchronized long waitInterval(final long amount) {
    return (amount <= avail) ? 0 : getWaitInterval(getLimit(), avail, amount);
  }

  /**
   * consume amount available units, amount could be a negative number
   *
   * @param amount the number of units to consume
   */
  public synchronized void consume(final long amount) {
    if (amount >= 0) {
      this.avail -= amount;
      if (this.avail < 0) {
        this.avail = 0;
      }
    } else {
      if (this.avail <= Long.MAX_VALUE + amount) {
        this.avail -= amount;
        this.avail = Math.min(this.avail, this.limit);
      } else {
        this.avail = this.limit;
      }
    }
  }
}
