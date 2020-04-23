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

package org.apache.iotdb.cluster.log;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;


public class HardState {

    private long currentTerm;
    private long voteFor;

    public HardState() {
    }

    public HardState(long currentTerm, long voteFor) {
        this.currentTerm = currentTerm;
        this.voteFor = voteFor;
    }

    public static HardState deserialize(ByteBuffer buffer) {
        HardState res = new HardState();
        res.currentTerm = ReadWriteIOUtils.readLong(buffer);
        res.voteFor = ReadWriteIOUtils.readLong(buffer);
        return res;
    }

    public ByteBuffer serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 2);
        byteBuffer.putLong(currentTerm);
        byteBuffer.putLong(voteFor);
        byteBuffer.flip();
        return byteBuffer;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public long getVoteFor() {
        return voteFor;
    }

    public void setVoteFor() {
        this.voteFor = voteFor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HardState)) {
            return false;
        }
        HardState that = (HardState) o;
        return new EqualsBuilder()
            .append(currentTerm, that.currentTerm)
            .append(voteFor, that.voteFor)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(currentTerm)
            .append(voteFor)
            .toHashCode();
    }
}
