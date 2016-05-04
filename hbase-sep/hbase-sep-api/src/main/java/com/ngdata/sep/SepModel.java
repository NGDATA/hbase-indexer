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

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Defines methods for adding and removing subscriptions on the Side-Effect Processor (SEP).
 */
public interface SepModel {

    /** Configuration key for storing the path of the root ZooKeeper node. */
    public static final String ZK_ROOT_NODE_CONF_KEY = "hbasesep.zookeeper.znode.parent";

    /** Configuration key for the batch size of SepEventExecutor. */
    public static final String EVENT_EXECUTOR_BATCH_SIZE_CONF_KEY = "hbasesep.event.executor.batch.size";

    /** Default root ZooKeeper node */
    public static final String DEFAULT_ZK_ROOT_NODE = "/ngdata/sep/hbase-slave";

    /**
     * Adds a subscription.
     * 
     * @throws IllegalStateException if a subscription by that name already exists.
     */
    void addSubscription(String name) throws InterruptedException, KeeperException, IOException;

    /**
     * Adds a subscription, doesn't fail if a subscription by that name exists.
     */
    boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException;

    /**
     * Removes a subscription.
     * 
     * @throws IllegalStateException if no subscription by that name exists.
     */
    void removeSubscription(String name) throws IOException;

    /**
     * Removes a subscription, doesn't fail if there is no subscription with that name.
     */
    boolean removeSubscriptionSilent(String name) throws IOException;

    boolean hasSubscription(String name) throws IOException;
}
