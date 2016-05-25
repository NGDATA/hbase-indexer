/*
 * Copyright 2016 NGDATA nv
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

package com.ngdata.hbaseindexer.indexer;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing utility for asserting that a calculated value <i>eventually</i> matches the expected value.
 * <p/>
 * This class should be used for situations where asynchronous processing should lead to the expected end result, but
 * it is difficult (or impossible) to know exactly when the required asynchronous processing is complete. The
 * strategy taken here is repeatedly testing the given situation (within a default timeout of 30 seconds) until the
 * expected result is reached.
 * <p/>
 * The various methods in this class take a {@link Callable} which calculates the value to be
 * checked. This <tt>Callable</tt> should be completely idempotent. The methods will all thrown an
 * {@link AssertionError} if the desired return value is not achieved within the timeout.
 */
public class Waiter {

    private static final long DEFAULT_WAIT_TIMEOUT = 30000L;

    /**
     * Repeatedly execute the given {@link Callable} until it returns true.
     *
     * @param condition a boolean-returning Callable which checks for the required result of a test
     */
    public static void waitFor(Callable<Boolean> condition) throws Exception {
        long sleepUntil = System.currentTimeMillis() + DEFAULT_WAIT_TIMEOUT;
        while (!condition.call() && System.currentTimeMillis() < sleepUntil) {
            Thread.sleep(20);
        }
        assertTrue(condition.call());
    }

    /**
     * Repeatedly execute the given {@link Callable} until it returns true, failing with the given failure message if
     * the callable doesn't return <tt>true</tt> within the timeout.
     *
     * @param condition   a boolean-returning Callable which checks for the required result of a test
     * @param failMessage the failure message to show if the callable doesn't return <tt>true</tt> within the timeout
     */
    public static void waitFor(Callable<Boolean> condition, String failMessage) throws Exception {
        long sleepUntil = System.currentTimeMillis() + DEFAULT_WAIT_TIMEOUT;
        while (!condition.call() && System.currentTimeMillis() < sleepUntil) {
            Thread.sleep(20);
        }
        if (!condition.call()) {
            fail(failMessage);
        }
    }

    /**
     * Repeatedly execute the given {@link Callable} until its return value is equal to expected value.
     *
     * @param expectedValue the return expected value from the callable
     * @param testValue     callable which calculates the value to be checked
     */
    public static <T> void waitFor(T expectedValue, Callable<T> testValue) throws Exception {
        long sleepUntil = System.currentTimeMillis() + DEFAULT_WAIT_TIMEOUT;
        while (!expectedValue.equals(testValue.call()) && System.currentTimeMillis() < sleepUntil) {
            Thread.sleep(20);
        }
        assertEquals(expectedValue, testValue.call());
    }
}
