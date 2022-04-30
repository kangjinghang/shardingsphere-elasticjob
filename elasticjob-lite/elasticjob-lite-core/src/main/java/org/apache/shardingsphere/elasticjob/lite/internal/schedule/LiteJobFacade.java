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

package org.apache.shardingsphere.elasticjob.lite.internal.schedule;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.executor.JobFacade;
import org.apache.shardingsphere.elasticjob.infra.context.TaskContext;
import org.apache.shardingsphere.elasticjob.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.infra.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.failover.FailoverService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ExecutionContextService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ExecutionService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.tracing.JobTracingEventBus;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lite job facade. 为作业提供内部服务的门面类
 */
@Slf4j
public final class LiteJobFacade implements JobFacade {
    // 作业配置服务
    private final ConfigurationService configService;
    // 作业分片服务
    private final ShardingService shardingService;
    // 作业运行时上下文服务
    private final ExecutionContextService executionContextService;
    // 执行作业服务
    private final ExecutionService executionService;
    // 作业失效转移服务
    private final FailoverService failoverService;
    // 作业监听器数组
    private final Collection<ElasticJobListener> elasticJobListeners;
    // 作业事件总线
    private final JobTracingEventBus jobTracingEventBus;
    
    public LiteJobFacade(final CoordinatorRegistryCenter regCenter, final String jobName, final Collection<ElasticJobListener> elasticJobListeners, final TracingConfiguration<?> tracingConfig) {
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionContextService = new ExecutionContextService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        this.elasticJobListeners = elasticJobListeners.stream().sorted(Comparator.comparingInt(ElasticJobListener::order)).collect(Collectors.toList());
        this.jobTracingEventBus = null == tracingConfig ? new JobTracingEventBus() : new JobTracingEventBus(tracingConfig);
    }
    
    @Override
    public JobConfiguration loadJobConfiguration(final boolean fromCache) {
        return configService.load(fromCache);
    }
    
    @Override
    public void checkJobExecutionEnvironment() throws JobExecutionEnvironmentException {
        configService.checkMaxTimeDiffSecondsTolerable();
    }
    
    @Override
    public void failoverIfNecessary() {
        if (configService.load(true).isFailover()) {
            failoverService.failoverIfNecessary();
        }
    }
    // 注册作业启动信息
    @Override
    public void registerJobBegin(final ShardingContexts shardingContexts) {
        executionService.registerJobBegin(shardingContexts);
    }
    // 注册作业完成信息
    @Override
    public void registerJobCompleted(final ShardingContexts shardingContexts) {
        executionService.registerJobCompleted(shardingContexts);
        if (configService.load(true).isFailover()) {
            failoverService.updateFailoverComplete(shardingContexts.getShardingItemParameters().keySet()); // 更新执行完毕失效转移的分片项状态
        }
    }
    // 可以看到作业执行器( ElasticJobExecutor ) 执行作业时，会获取当前作业服务器的分片上下文进行执行
    @Override
    public ShardingContexts getShardingContexts() {
        boolean isFailover = configService.load(true).isFailover();
        if (isFailover) { // JobConfiguration 设置了 failover = true 的话，如果有失效转移分片，先处理
            List<Integer> failoverShardingItems = failoverService.getLocalFailoverItems(); // 获取运行在本作业节点的失效转移分片项集合
            if (!failoverShardingItems.isEmpty()) { // 运行在本作业节点的失效转移分片项集合不为空
                return executionContextService.getJobShardingContext(failoverShardingItems);
            }
        } // 走到这里，说明本机作业节点不存在抓取的失效转移分片项
        shardingService.shardingIfNecessary();  // 作业分片，如果需要分片且当前节点为主节点
        List<Integer> shardingItems = shardingService.getLocalShardingItems(); // 获取分配了（分配在本机的）的作业分片项
        if (isFailover) { // JobConfiguration 设置了 failover = true
            shardingItems.removeAll(failoverService.getLocalTakeOffItems()); // 移除 分配在本机的失效转移（来自其他失效作业节点的）的作业分片项目。举个例子，作业节点A持有作业分片项[0, 1]，此时异常断网，导致[0, 1]被作业节点B失效转移抓取，此时若作业节点A恢复，作业分片项[0, 1]依然属于作业节点A，但是可能已经在作业节点B执行，因此需要从B进行移除，避免多节点运行相同的作业分片项。
        }
        shardingItems.removeAll(executionService.getDisabledItems(shardingItems)); // 移除 被禁用的作业分片项
        return executionContextService.getJobShardingContext(shardingItems); // 获取当前作业服务器分片上下文
    }
    // 当分配的作业分片项里存在【任意】一个分片正在运行中，所有被分配的分片项都会被设置为错过执行( misfire )，并不执行这些作业分片了。因为如果不进行跳过，则可能导致多个作业服务器同时运行某个作业分片。该功能依赖作业配置监控作业运行时状态( monitorExecution = true )时生效。
    @Override
    public boolean misfireIfRunning(final Collection<Integer> shardingItems) {
        return executionService.misfireIfHasRunningItems(shardingItems);
    }
    
    @Override
    public void clearMisfire(final Collection<Integer> shardingItems) {
        executionService.clearMisfire(shardingItems);
    }
    // 配置开启了错过任务重执行 && 合适继续运行 && 所执行的作业分片存在被错过( misfire )
    @Override
    public boolean isExecuteMisfired(final Collection<Integer> shardingItems) {
        return configService.load(true).isMisfire() && !isNeedSharding() && !executionService.getMisfiredJobItems(shardingItems).isEmpty();
    }
    
    @Override
    public boolean isNeedSharding() {
        return shardingService.isNeedSharding(); // 合适继续运行
    }
    
    @Override
    public void beforeJobExecuted(final ShardingContexts shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.beforeJobExecuted(shardingContexts);
        }
    }
    
    @Override
    public void afterJobExecuted(final ShardingContexts shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.afterJobExecuted(shardingContexts);
        }
    }
    
    @Override
    public void postJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        jobTracingEventBus.post(jobExecutionEvent);
    }
    
    @Override
    public void postJobStatusTraceEvent(final String taskId, final State state, final String message) {
        TaskContext taskContext = TaskContext.from(taskId); // 对作业任务ID( taskId ) 解析，获取任务上下文
        jobTracingEventBus.post(new JobStatusTraceEvent(taskContext.getMetaInfo().getJobName(), taskContext.getId(),
                taskContext.getSlaveId(), Source.LITE_EXECUTOR, taskContext.getType().name(), taskContext.getMetaInfo().getShardingItems().toString(), state, message));
        if (!Strings.isNullOrEmpty(message)) {
            log.trace(message);
        }
    }
}
