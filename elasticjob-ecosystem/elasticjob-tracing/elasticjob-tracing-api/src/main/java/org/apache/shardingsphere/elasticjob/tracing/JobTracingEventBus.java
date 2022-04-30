/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.tracing;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;
import org.apache.shardingsphere.elasticjob.tracing.event.JobEvent;
import org.apache.shardingsphere.elasticjob.tracing.exception.TracingConfigurationException;
import org.apache.shardingsphere.elasticjob.tracing.listener.TracingListenerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Job tracing event bus. 作业事件总线，提供了注册监听器、发布事件两个方法。
 */
@Slf4j
public final class JobTracingEventBus {
    // 线程池执行服务对象
    private static final ExecutorService EXECUTOR_SERVICE;
    // 事件总线
    private final EventBus eventBus;
    // 是否注册作业监听器
    private volatile boolean isRegistered;

    static {
        EXECUTOR_SERVICE = createExecutorService(Runtime.getRuntime().availableProcessors() * 2);
    }
    
    public JobTracingEventBus() {
        eventBus = null;
    }
    
    public JobTracingEventBus(final TracingConfiguration<?> tracingConfig) {
        eventBus = new AsyncEventBus(EXECUTOR_SERVICE); // 创建 异步事件总线。注册在其上面的监听器是异步监听执行，事件发布无需阻塞等待监听器执行完逻辑，所以对性能不存在影响。
        register(tracingConfig);  // 注册 事件监听器
    }
    
    private static ExecutorService createExecutorService(final int threadSize) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadSize, threadSize, 5L, TimeUnit.MINUTES, 
                new LinkedBlockingQueue<>(), new BasicThreadFactory.Builder().namingPattern(String.join("-", "job-event", "%s")).build());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return MoreExecutors.listeningDecorator(MoreExecutors.getExitingExecutorService(threadPoolExecutor));
    }
    // 私有( private )方法，只能使用 TracingConfiguration 创建事件监听器注册。当不传递该配置时，意味着不开启事件追踪功能
    private void register(final TracingConfiguration<?> tracingConfig) {
        try {
            eventBus.register(TracingListenerFactory.getListener(tracingConfig));
            isRegistered = true;
        } catch (final TracingConfigurationException ex) {
            log.error("Elastic job: create tracing listener failure, error is: ", ex);
        }
    }
    
    /**
     * Post event. 发布作业事件
     *
     * @param event job event
     */
    public void post(final JobEvent event) {
        if (isRegistered && !EXECUTOR_SERVICE.isShutdown()) {
            eventBus.post(event);
        }
    }
}
