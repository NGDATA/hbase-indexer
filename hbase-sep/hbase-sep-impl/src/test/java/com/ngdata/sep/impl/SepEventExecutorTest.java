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
package com.ngdata.sep.impl;

import com.google.common.collect.Lists;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.util.concurrent.WaitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SepEventExecutorTest {

    private SepMetrics sepMetrics;
    private List<ThreadPoolExecutor> executors;

    @Before
    public void setUp() {
        sepMetrics = mock(SepMetrics.class);
        executors = Lists.newArrayListWithCapacity(10);
        for (int i = 0; i < 10; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
    }
    
    @After
    public void tearDown() {
        for (ThreadPoolExecutor executor : executors) {
            executor.shutdownNow();
        }
    }

    private SepEvent createSepEvent(int row) {
        SepEvent sepEvent = mock(SepEvent.class);
        when(sepEvent.getRow()).thenReturn(Bytes.toBytes(String.valueOf(row)));
        return sepEvent;
    }
    
    private List<ThreadPoolExecutor> getExecutors(int numThreads) {
        return executors.subList(0, numThreads);
    }

    @Test
    public void testScheduleSepEvent() throws InterruptedException {
        RecordingEventListener eventListener = new RecordingEventListener();
        SepEventExecutor executor = new SepEventExecutor(eventListener, getExecutors(2), 1, sepMetrics);
        final int NUM_EVENTS = 10;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleSepEvent(createSepEvent(i));
        }

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());
    }

    @Test
    public void testScheduleSepEvent_NotFullBatch() throws InterruptedException {
        RecordingEventListener eventListener = new RecordingEventListener();
        SepEventExecutor executor = new SepEventExecutor(eventListener, getExecutors(2), 100, sepMetrics);
        final int NUM_EVENTS = 10;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleSepEvent(createSepEvent(i));
        }

        Thread.sleep(50); // Give us a little bit of time to ensure nothing cam through yet

        assertTrue(eventListener.receivedEvents.isEmpty());

        List<Future<?>> futures = executor.flush();
        assertFalse(futures.isEmpty());

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());
    }

    @Test
    public void testScheduleSepEvent_EventOverflow() throws InterruptedException {
        DelayingEventListener eventListener = new DelayingEventListener();
        SepEventExecutor executor = new SepEventExecutor(eventListener, getExecutors(1), 1, sepMetrics);
        final int NUM_EVENTS = 50;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleSepEvent(createSepEvent(i));
        }
        List<Future<?>> futures = executor.flush();

        // We're running with a single thread and no batching, so there should be just as many
        // futures as there are events
        assertEquals(NUM_EVENTS, futures.size());

        Thread.sleep(500);

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());

    }

    static class RecordingEventListener implements EventListener {

        List<SepEvent> receivedEvents = Lists.newArrayList();

        @Override
        public synchronized void processEvents(List<SepEvent> events) {
            receivedEvents.addAll(events);
        }

    }

    static class DelayingEventListener implements EventListener {

        List<SepEvent> receivedEvents = Collections.synchronizedList(Lists.<SepEvent> newArrayList());

        @Override
        public void processEvents(List<SepEvent> events) {
            for (SepEvent event : events) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                receivedEvents.add(event);
            }
        }

    }
}
