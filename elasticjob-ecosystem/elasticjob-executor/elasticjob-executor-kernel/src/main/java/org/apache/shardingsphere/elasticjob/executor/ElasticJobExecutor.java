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

package org.apache.shardingsphere.elasticjob.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.error.handler.JobErrorHandler;
import org.apache.shardingsphere.elasticjob.executor.context.ExecutorContext;
import org.apache.shardingsphere.elasticjob.executor.item.JobItemExecutor;
import org.apache.shardingsphere.elasticjob.executor.item.JobItemExecutorFactory;
import org.apache.shardingsphere.elasticjob.infra.env.IpUtils;
import org.apache.shardingsphere.elasticjob.infra.exception.ExceptionUtils;
import org.apache.shardingsphere.elasticjob.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * ElasticJob executor. 作业执行逻辑，使用 具体的JobItemExecutor 执行作业
 */
@Slf4j
public final class ElasticJobExecutor {
    
    private final ElasticJob elasticJob;
    // 作业门面对象
    private final JobFacade jobFacade;
    
    private final JobItemExecutor jobItemExecutor;
    
    private final ExecutorContext executorContext;
    // 分片错误信息集合，key：分片序号
    private final Map<Integer, String> itemErrorMessages;
    
    public ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(elasticJob, jobConfig, jobFacade, JobItemExecutorFactory.getExecutor(elasticJob.getClass()));
    }
    
    public ElasticJobExecutor(final String type, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(null, jobConfig, jobFacade, JobItemExecutorFactory.getExecutor(type));
    }
    
    private ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final JobItemExecutor jobItemExecutor) {
        this.elasticJob = elasticJob;
        this.jobFacade = jobFacade;
        this.jobItemExecutor = jobItemExecutor;
        executorContext = new ExecutorContext(jobFacade.loadJobConfiguration(true)); // 加载 作业配置
        itemErrorMessages = new ConcurrentHashMap<>(jobConfig.getShardingTotalCount(), 1);  // 设置 分片错误信息集合
    }
    
    /**
     * Execute job. 核心方法
     */
    public void execute() {
        JobConfiguration jobConfig = jobFacade.loadJobConfiguration(true);
        executorContext.reloadIfNecessary(jobConfig);
        JobErrorHandler jobErrorHandler = executorContext.get(JobErrorHandler.class);
        try {
            jobFacade.checkJobExecutionEnvironment(); // 检查作业执行环境，校验本机时间是否合法
        } catch (final JobExecutionEnvironmentException cause) {
            jobErrorHandler.handleException(jobConfig.getJobName(), cause); // 当校验本机时间不合法时，抛出异常。若使用 DefaultJobExceptionHandler 作为异常处理，只打印日志，不会终止作业执行。如果你的作业对时间精准度有比较高的要求，期望作业终止执行，可以自定义 JobExceptionHandler 实现对异常的处理。
        }
        ShardingContexts shardingContexts = jobFacade.getShardingContexts(); // 获取 当前作业服务器的分片上下文，作业获得其所分配执行的分片项
        jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_STAGING, String.format("Job '%s' execute begin.", jobConfig.getJobName())); // 发布作业状态追踪事件(State.TASK_STAGING)
        if (jobFacade.misfireIfRunning(shardingContexts.getShardingItemParameters().keySet())) { // 重叠作业检查，设置 misfire，通过错过重调度机制执行。跳过正在运行中的被错过执行的作业
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format( // 发布作业状态追踪事件(State.TASK_FINISHED)
                    "Previous job '%s' - shardingItems '%s' is still running, misfired job will start after previous job completed.", jobConfig.getJobName(),
                    shardingContexts.getShardingItemParameters().keySet()));
            return; // 当分配的作业分片项里存在【任意】一个分片正在运行中，所有被分配的分片项都会被设置为错过执行( misfire )，并不执行这些作业分片了。因为如果不进行跳过，则可能导致多个作业服务器同时运行某个作业分片。该功能依赖作业配置监控作业运行时状态( monitorExecution = true )时生效。
        }
        try {
            jobFacade.beforeJobExecuted(shardingContexts); // 执行 作业执行前的方法
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
        execute(jobConfig, shardingContexts, ExecutionSource.NORMAL_TRIGGER); // 执行 【普通触发的作业】。分两个执行场景：quatz调起，执行常规分片作业；失效转移触发，执行失效转移分片
        // while，防御性编程。#isExecuteMisfired(...) 判断使用内存缓存的数据，而该数据的更新依赖 Zookeeper 通知进行异步更新，可能因为各种情况，例如网络，数据可能未及时更新导致数据不一致。使用 while(...) 进行防御编程，保证内存缓存的数据已经更新。
        while (jobFacade.isExecuteMisfired(shardingContexts.getShardingItemParameters().keySet())) { // 执行 【被跳过触发】的作业。错过重执行有两个来源，1.重叠执行转missfired，但分片逻辑没有考虑该来源missfired分片；2.失效转移转missfired
            jobFacade.clearMisfire(shardingContexts.getShardingItemParameters().keySet()); // 清除分配的作业分片项被错过执行的标志
            execute(jobConfig, shardingContexts, ExecutionSource.MISFIRE); // 普通触发的作业执行完了，现在开始执行 被跳过触发的作业
        }
        jobFacade.failoverIfNecessary(); // 作业在执行完分配给自己的作业分片项，执行 【作业失效转移】
        try {
            jobFacade.afterJobExecuted(shardingContexts); // 执行 作业执行后的方法
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }
    // 执行作业 executionSource（NORMAL_TRIGGER, MISFIRE, FAILOVER）
    private void execute(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        if (shardingContexts.getShardingItemParameters().isEmpty()) {  // 无可执行的分片，发布作业状态追踪事件(State.TASK_FINISHED)
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format("Sharding item for job '%s' is empty.", jobConfig.getJobName()));
            return;
        }
        jobFacade.registerJobBegin(shardingContexts); // 注册作业启动信息，作业分片执行前，写入 zk 分片项 running 状态，znode/{itemNum}/running
        String taskId = shardingContexts.getTaskId();
        jobFacade.postJobStatusTraceEvent(taskId, State.TASK_RUNNING, ""); // 发布作业状态追踪事件(State.TASK_RUNNING)
        try {
            process(jobConfig, shardingContexts, executionSource); // 分片执行
        } finally {
            // TODO Consider increasing the status of job failure, and how to handle the overall loop of job failure
            jobFacade.registerJobCompleted(shardingContexts); // 注册作业完成信息，作业执行结束，删除 zk 分片项 znode running，删除失效转移 znode
            if (itemErrorMessages.isEmpty()) { // 根据是否有异常，发布作业状态追踪事件(State.TASK_FINISHED / State.TASK_ERROR)
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_FINISHED, "");
            } else {
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_ERROR, itemErrorMessages.toString());
                itemErrorMessages.clear();
            }
        }
    }
    // 分片执行 executionSource（NORMAL_TRIGGER, MISFIRE, FAILOVER）
    private void process(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        Collection<Integer> items = shardingContexts.getShardingItemParameters().keySet();
        if (1 == items.size()) { // 单分片（当前作业服务器只被分配了一个分片），直接执行
            int item = shardingContexts.getShardingItemParameters().keySet().iterator().next();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, item);
            process(jobConfig, shardingContexts, item, jobExecutionEvent); // 执行一个作业
            return;
        } // 分配单分片项时，直接执行，无需使用线程池，性能更优。
        CountDownLatch latch = new CountDownLatch(items.size()); // 多分片（当前作业服务器被分配了多个分片），并行执行
        for (int each : items) {
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, each);
            ExecutorService executorService = executorContext.get(ExecutorService.class); // 获取到线程池
            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(() -> { // 分配多分片项时，使用线程池并发执行，通过 CountDownLatch 实现等待分片项全部执行完成。
                try {
                    process(jobConfig, shardingContexts, each, jobExecutionEvent); // 执行一个作业
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(); // 等待多分片全部完成
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    
    @SuppressWarnings("unchecked")
    private void process(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final int item, final JobExecutionEvent startEvent) {
        jobFacade.postJobExecutionEvent(startEvent); // 发布执行事件(开始)
        log.trace("Job '{}' executing, item is: '{}'.", jobConfig.getJobName(), item);
        JobExecutionEvent completeEvent;
        try {
            jobItemExecutor.process(elasticJob, jobConfig, jobFacade, shardingContexts.createShardingContext(item)); // 执行单个作业，不同作业执行器实现类通过实现 jobItemExecutor#process() 抽象方法，实现对单个分片项作业的处理
            completeEvent = startEvent.executionSuccess();
            log.trace("Job '{}' executed, item is: '{}'.", jobConfig.getJobName(), item);
            jobFacade.postJobExecutionEvent(completeEvent); // 发布执行事件(成功)
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON
            completeEvent = startEvent.executionFailure(ExceptionUtils.transform(cause));
            jobFacade.postJobExecutionEvent(completeEvent); // 发布执行事件(失败)
            itemErrorMessages.put(item, ExceptionUtils.transform(cause));
            JobErrorHandler jobErrorHandler = executorContext.get(JobErrorHandler.class);
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }
    
    /**
     * Shutdown executor.
     */
    public void shutdown() {
        executorContext.shutdown();
    }
}
