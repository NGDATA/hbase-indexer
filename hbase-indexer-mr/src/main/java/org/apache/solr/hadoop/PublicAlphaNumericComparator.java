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
package org.apache.solr.hadoop;

import java.util.Comparator;

/**
 * Nasty trick to access the non public {@link AlphaNumericComparator} class.
 */
public class PublicAlphaNumericComparator implements Comparator {
    private final static AlphaNumericComparator INSTANCE = new AlphaNumericComparator();

    @Override
    public int compare(Object o1, Object o2) {
        return INSTANCE.compare(o1, o2);
    }
}
