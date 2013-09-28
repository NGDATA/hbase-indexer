/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.sep.impl.fork;

public class ReplicationSourceInfo implements ReplicationSourceInfoMBean {
    private String sleepReason;
    private int sleepMultiplier;
    private long timestampLastSleep;
    private long timestampLastShippedOp;
    private int selectedPeerCount;

    public void setSleepInfo(String sleepReason, int sleepMultiplier) {
        this.sleepReason = sleepReason;
        this.sleepMultiplier = sleepMultiplier;
        this.timestampLastSleep = System.currentTimeMillis();
    }

    public String getSleepReason() {
        return sleepReason;
    }

    public int getSleepMultiplier() {
        return sleepMultiplier;
    }

    public long getTimestampLastSleep() {
        return timestampLastSleep;
    }

    public long getTimestampLastShippedOp() {
        return timestampLastShippedOp;
    }

    public void setTimestampLastShippedOp(long timestampLastShippedOp) {
        this.timestampLastShippedOp = timestampLastShippedOp;
    }

    public int getSelectedPeerCount() {
        return selectedPeerCount;
    }

    public void setSelectedPeerCount(int selectedPeerCount) {
        this.selectedPeerCount = selectedPeerCount;
    }
}
