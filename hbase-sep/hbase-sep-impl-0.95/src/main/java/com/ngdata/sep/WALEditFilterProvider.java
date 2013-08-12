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

/**
 * Provider of {@link WALEditFilter}s via the {@code ServiceLoader} system.
 */
public interface WALEditFilterProvider {

    /**
     * Get a {@code WALEditFilter} for the given SEP subscription id.
     * <p>
     * If no {@code WALEditFilter} is available for the given subscription id, null is returned.
     *
     * @param subscriptionId The id of the subscription for which the WALEditFilter is to be loaded
     * @return The edit filter, or null if no applicable filter is available from this loader
     */
    WALEditFilter getWALEditFilter(String subscriptionId);

}
