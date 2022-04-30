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

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State;

import java.util.Collection;

/**
 * Job facade.
 */
public interface JobFacade {
    
    /**
     * Load job configuration.
     * 
     * @param fromCache load from cache or not
     * @return job configuration
     */
    JobConfiguration loadJobConfiguration(boolean fromCache);
    
    /**
     * Check job execution environment.
     * 
     * @throws JobExecutionEnvironmentException job execution environment exception
     */
    void checkJobExecutionEnvironment() throws JobExecutionEnvironmentException;
    
    /**
     * Failover If necessary.
     */
    void failoverIfNecessary();
    
    /**
     * Register job begin. 注册作业启动信息
     *
     * @param shardingContexts sharding contexts
     */
    void registerJobBegin(ShardingContexts shardingContexts);
    
    /**
     * Register job completed. 注册作业完成信息
     *
     * @param shardingContexts sharding contexts
     */
    void registerJobCompleted(ShardingContexts shardingContexts);
    
    /**
     * Get sharding contexts. 获取 当前作业服务器的分片上下文，作业获得其所分配执行的分片项
     *
     * @return sharding contexts
     */
    ShardingContexts getShardingContexts();
    
    /**
     * Set task misfire flag. 设置分片为 misfire 标志
     * 当分配的作业分片项里存在【任意】一个分片正在运行中，所有被分配的分片项都会被设置为错过执行( misfire )，并不执行这些作业分片了。因为如果不进行跳过，则可能导致多个作业服务器同时运行某个作业分片。该功能依赖作业配置监控作业运行时状态( monitorExecution = true )时生效。
     * @param shardingItems sharding items to be set misfire flag 要被设置 misfire 标志的分片们
     * @return whether satisfy misfire condition  是否满足 misfire 条件
     */
    boolean misfireIfRunning(Collection<Integer> shardingItems);
    
    /**
     * Clear misfire flag.
     *
     * @param shardingItems sharding items to be cleared misfire flag
     */
    void clearMisfire(Collection<Integer> shardingItems);
    
    /**
     * Judge job whether need to execute misfire tasks. 判断 job 是否需要执行 misfire 任务
     * 
     * @param shardingItems sharding items
     * @return whether need to execute misfire tasks 是否需要执行 misfire 任务
     */
    boolean isExecuteMisfired(Collection<Integer> shardingItems);
    
    /**
     * Judge job whether need resharding.
     *
     * @return whether need resharding
     */
    boolean isNeedSharding();
    
    /**
     * Call before job executed. 执行作业执行前的方法
     *
     * @param shardingContexts sharding contexts
     */
    void beforeJobExecuted(ShardingContexts shardingContexts);
    
    /**
     * Call after job executed.
     *
     * @param shardingContexts sharding contexts
     */
    void afterJobExecuted(ShardingContexts shardingContexts);
    
    /**
     * Post job execution event. 发布作业执行事件
     *
     * @param jobExecutionEvent job execution event
     */
    void postJobExecutionEvent(JobExecutionEvent jobExecutionEvent);
    
    /**
     * Post job status trace event. 发布作业状态追踪事件
     *
     * @param taskId task Id
     * @param state job state
     * @param message job message
     */
    void postJobStatusTraceEvent(String taskId, State state, String message);
}
