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

package org.apache.shardingsphere.elasticjob.lite.internal.sharding;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.context.ShardingItemParameters;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Execution context service.
 */
public final class ExecutionContextService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ConfigurationService configService;
    
    public ExecutionContextService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
    }
    
    /**
     * Get job sharding context. 获取当前作业服务器分片上下文
     *
     * @param shardingItems sharding items
     * @return job sharding context
     */
    public ShardingContexts getJobShardingContext(final List<Integer> shardingItems) {
        JobConfiguration jobConfig = configService.load(false);
        removeRunningIfMonitorExecution(jobConfig.isMonitorExecution(), shardingItems);  // 移除 正在运行中的作业分片项
        if (shardingItems.isEmpty()) {
            return new ShardingContexts(buildTaskId(jobConfig, shardingItems), jobConfig.getJobName(), jobConfig.getShardingTotalCount(), 
                    jobConfig.getJobParameter(), Collections.emptyMap());
        }
        Map<Integer, String> shardingItemParameterMap = new ShardingItemParameters(jobConfig.getShardingItemParameters()).getMap(); // 使用 ShardingItemParameters 解析作业分片参数
        return new ShardingContexts(buildTaskId(jobConfig, shardingItems), jobConfig.getJobName(), jobConfig.getShardingTotalCount(),  // 创建作业任务ID( ShardingContexts.taskId )：
                jobConfig.getJobParameter(), getAssignedShardingItemParameterMap(shardingItems, shardingItemParameterMap)); // 获得当前作业节点的分片参数
    }
    // taskId = ${JOB_NAME} + @-@ + ${SHARDING_ITEMS} + @-@ + READY + @-@ + ${IP} + @-@ + ${PID}。例如：javaSimpleJob@-@0,1,2@-@READY@-@192.168.3.2@-@38330
    private String buildTaskId(final JobConfiguration jobConfig, final List<Integer> shardingItems) {
        JobInstance jobInstance = JobRegistry.getInstance().getJobInstance(jobName);
        String shardingItemsString = shardingItems.stream().map(Object::toString).collect(Collectors.joining(","));
        String jobInstanceId = null == jobInstance || null == jobInstance.getJobInstanceId() ? "127.0.0.1@-@1" : jobInstance.getJobInstanceId();
        return String.join("@-@", jobConfig.getJobName(), shardingItemsString, "READY", jobInstanceId); 
    }
    // 移除正在运行中的作业分片项
    private void removeRunningIfMonitorExecution(final boolean monitorExecution, final List<Integer> shardingItems) {
        if (!monitorExecution) {
            return;
        }
        List<Integer> runningShardingItems = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (isRunning(each)) {
                runningShardingItems.add(each);  // /${JOB_NAME}/sharding/${ITEM_ID}/running
            }
        }
        shardingItems.removeAll(runningShardingItems);
    }
    
    private boolean isRunning(final int shardingItem) {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.getRunningNode(shardingItem));
    }
    // 获得当前作业节点的分片参数
    private Map<Integer, String> getAssignedShardingItemParameterMap(final List<Integer> shardingItems, final Map<Integer, String> shardingItemParameterMap) {
        Map<Integer, String> result = new HashMap<>(shardingItems.size(), 1);
        for (int each : shardingItems) {
            result.put(each, shardingItemParameterMap.get(each));
        }
        return result;
    }
}
