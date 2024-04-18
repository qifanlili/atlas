/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.util;

import java.util.concurrent.*;

public class CustomThreadPool {
    private static CustomThreadPool instance;
    private static final int CORE_POOL_SIZE = 10;
    private static final int MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final long KEEP_ALIVE_TIME = 60L;
    private ThreadPoolExecutor threadPool;

    private CustomThreadPool() {
        threadPool = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static synchronized CustomThreadPool getInstance() {
        if (instance == null) {
            instance = new CustomThreadPool();
        }
        return instance;
    }

    public void execute(Runnable task) {
        threadPool.execute(task);
    }

    public ThreadPoolExecutor getExecutor() {
        return threadPool;
    }

    public void close() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("线程池未能完全关闭");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            threadPool.shutdownNow();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.close();
            instance = null;
        }
    }
}