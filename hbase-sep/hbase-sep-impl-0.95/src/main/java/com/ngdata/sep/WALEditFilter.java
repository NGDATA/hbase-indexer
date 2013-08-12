/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep;

import org.apache.hadoop.hbase.regionserver.wal.HLog;

/**
 * Filter for removing non-applicable {@code KeyValue}s from {@code WALEdit}s before they are replicated via HBase replication.
 */
public interface WALEditFilter {

    /**
     * Apply filtering to a WALEdit.
     * <p>
     * All KeyValues that are to not be replicated are removed from the WALEdit in this call.
     *
     * @param walEdit edit from which KeyValues can be removed before replication
     */
    void apply(HLog.Entry walEdit);

}
