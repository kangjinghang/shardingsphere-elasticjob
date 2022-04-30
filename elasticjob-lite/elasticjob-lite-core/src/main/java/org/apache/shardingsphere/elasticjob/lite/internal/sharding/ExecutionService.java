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
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Execution service.
 */
public final class ExecutionService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ConfigurationService configService;
    
    public ExecutionService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
    }
        
    /**
     * Register job begin. 注册作业启动信息
     * 
     * @param shardingContexts sharding contexts
     */
    public void registerJobBegin(final ShardingContexts shardingContexts) {
        JobRegistry.getInstance().setJobRunning(jobName, true);
        if (!configService.load(true).isMonitorExecution()) { // 仅当作业配置设置监控作业运行时状态( LiteJobConfiguration.monitorExecution = true )时，记录作业运行状态
            return;
        }
        for (int each : shardingContexts.getShardingItemParameters().keySet()) {
            jobNodeStorage.fillEphemeralJobNode(ShardingNode.getRunningNode(each), ""); // 记录分配的作业分片项正在运行中，写入 zk 分片项 running 状态
        }
    }
    
    /**
     * Register job completed.  注册作业完成信息
     * 
     * @param shardingContexts sharding contexts
     */
    public void registerJobCompleted(final ShardingContexts shardingContexts) {
        JobRegistry.getInstance().setJobRunning(jobName, false);
        if (!configService.load(true).isMonitorExecution()) { // 仅当作业配置设置监控作业运行时状态( LiteJobConfiguration.monitorExecution = true )，移除作业运行状态。
            return;
        }
        for (int each : shardingContexts.getShardingItemParameters().keySet()) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getRunningNode(each)); // 移除分配的作业分片项正在运行中的标记，表示作业分片项不在运行中状态。
        }
    }
    
    /**
     * Clear all running info.
     */
    public void clearAllRunningInfo() {
        clearRunningInfo(getAllItems());
    }
    
    /**
     * Clear running info.
     * 
     * @param items sharding items which need to be cleared
     */
    public void clearRunningInfo(final List<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getRunningNode(each));
        }
    }
    
    /**
     * Judge has running items or not.
     *
     * @param items sharding items need to be judged
     * @return has running items or not
     */
    public boolean hasRunningItems(final Collection<Integer> items) {
        JobConfiguration jobConfig = configService.load(true);
        if (!jobConfig.isMonitorExecution()) {
            return false;
        }
        for (int each : items) {
            if (jobNodeStorage.isJobNodeExisted(ShardingNode.getRunningNode(each))) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Judge has running items or not.
     *
     * @return has running items or not
     */
    public boolean hasRunningItems() {
        return hasRunningItems(getAllItems());
    }
    
    private List<Integer> getAllItems() {
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        List<Integer> result = new ArrayList<>(shardingTotalCount);
        for (int i = 0; i < shardingTotalCount; i++) {
            result.add(i);
        }
        return result;
    }
    
    /**
     * Set misfire flag if sharding items still running.  如果这些分片仍在运行状态，分片将被设置为 misfired 标志
     * 当分配的作业分片项里存在【任意】一个分片正在运行中，所有被分配的分片项都会被设置为错过执行( misfire )，并不执行这些作业分片了。因为如果不进行跳过，则可能导致多个作业服务器同时运行某个作业分片。该功能依赖作业配置监控作业运行时状态( monitorExecution = true )时生效。
     * @param items sharding items need to be set misfire flag
     * @return is misfired for this schedule time or not 是否在调度时间内 misfire
     */
    public boolean misfireIfHasRunningItems(final Collection<Integer> items) {
        if (!hasRunningItems(items)) { // 如果没有正在运行的分片，返回 false，代表没有分片在调度时间内 misfire 了
            return false;
        }
        setMisfire(items); // 否则，将分片设置为 misfired 标志
        return true; // 返回 true，代表有分片在调度时间内 misfire 了
    }
    
    /**
     * Set misfire flag if sharding items still running. 如果这些分片仍在运行，分片将被设置为 misfire 标志
     *
     * @param items sharding items need to be set misfire flag
     */
    public void setMisfire(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.getMisfireNode(each));
        }
    }
    
    /**
     * Get misfired job sharding items. 获取被错过执行的分片们
     * 
     * @param items sharding items need to be judged
     * @return misfired job sharding items
     */
    public List<Integer> getMisfiredJobItems(final Collection<Integer> items) {
        List<Integer> result = new ArrayList<>(items.size());
        for (int each : items) {
            if (jobNodeStorage.isJobNodeExisted(ShardingNode.getMisfireNode(each))) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * Clear misfire flag.
     * 
     * @param items sharding items need to be cleared
     */
    public void clearMisfire(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getMisfireNode(each));
        }
    }
    
    /**
     * Get disabled sharding items. 获取禁用的任务分片项
     *
     * @param items sharding items need to be got 需要获取禁用的任务分片项
     * @return disabled sharding items 禁用的任务分片项
     */
    public List<Integer> getDisabledItems(final List<Integer> items) {
        List<Integer> result = new ArrayList<>(items.size());
        for (int each : items) {
            if (jobNodeStorage.isJobNodeExisted(ShardingNode.getDisabledNode(each))) { // /${JOB_NAME}/sharding/${ITEM_ID}/disabled
                result.add(each);
            }
        }
        return result;
    }
}
