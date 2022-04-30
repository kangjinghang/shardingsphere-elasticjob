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

package org.apache.shardingsphere.elasticjob.lite.internal.failover;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobScheduleController;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingNode;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Failover service.
 */
@Slf4j
public final class FailoverService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ShardingService shardingService;
    
    public FailoverService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }
    
    /**
     * set crashed failover flag. 设置失效的分片项标记 /${JOB_NAME}/leader/failover/items/${ITEM_ID}。该数据节点为永久节点，存储空串( "" )
     * 
     * @param item crashed job item
     */
    public void setCrashedFailoverFlag(final int item) {
        if (!isFailoverAssigned(item)) { // 判断
            jobNodeStorage.createJobNodeIfNeeded(FailoverNode.getItemsNode(item)); // /${JOB_NAME}/leader/failover/items/${ITEM_ID}
        }
    }

    /**
     * set crashed failover flag directly.
     *
     * @param item crashed item
     */
    public void setCrashedFailoverFlagDirectly(final int item) {
        jobNodeStorage.createJobNodeIfNeeded(FailoverNode.getItemsNode(item)); // 写入 /${JOB_NAME}/leader/failover/items/{ITEM_ID}，需要失效转移的分片, 待 failoverIfNecessary 抓取
    }

    private boolean isFailoverAssigned(final Integer item) {
        return jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item));
    }
    
    /**
     * Failover if necessary. 如果需要失效转移, 则执行作业失效转移
     */
    public void failoverIfNecessary() {
        if (needFailover()) { // 判断是否满足失效转移条件
            jobNodeStorage.executeInLeader(FailoverNode.LATCH, new FailoverLeaderExecutionCallback());
        }
    }
    // 判断是否满足失效转移条件
    private boolean needFailover() {  // ${JOB_NAME}/leader/failover/items/${ITEM_ID} 有失效转移的作业分片项 && 当前作业不在运行中
        return jobNodeStorage.isJobNodeExisted(FailoverNode.ITEMS_ROOT) && !jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).isEmpty()
                && !JobRegistry.getInstance().isJobRunning(jobName); // 作业节点【空闲】的定义。启用失效转移功能可以在本次作业执行过程中，监测其他作业服务器【空闲】，抓取未完成的孤儿分片项执行
    }
    
    /**
     * Update sharding items status when failover execution complete.
     * 
     * @param items sharding items of failover execution completed
     */
    public void updateFailoverComplete(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(each));
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutingFailoverNode(each));
        }
    }
    
    /**
     * Get failover items.
     *
     * @param jobInstanceId job instance ID
     * @return failover items
     */
    public List<Integer> getFailoverItems(final String jobInstanceId) {
        List<String> items = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT);
        List<Integer> result = new ArrayList<>(items.size());
        for (String each : items) {
            int item = Integer.parseInt(each);
            String node = FailoverNode.getExecutionFailoverNode(item);
            if (jobNodeStorage.isJobNodeExisted(node) && jobInstanceId.equals(jobNodeStorage.getJobNodeDataDirectly(node))) {
                result.add(item);
            }
        }
        Collections.sort(result);
        return result;
    }

    /**
     * Get failovering items.
     *
     * @param jobInstanceId job instance ID
     * @return failovering items
     */
    public List<Integer> getFailoveringItems(final String jobInstanceId) {
        List<String> items = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT);
        List<Integer> result = new ArrayList<>(items.size());
        for (String each : items) {
            int item = Integer.parseInt(each);
            String node = FailoverNode.getExecutingFailoverNode(item); // ${JOB_NAME}/sharding/${ITEM_ID}/failover
            if (jobNodeStorage.isJobNodeExisted(node) && jobInstanceId.equals(jobNodeStorage.getJobNodeDataDirectly(node))) {
                result.add(item);
            }
        }
        Collections.sort(result);
        return result;
    }
    
    /**
     * Get failover items which execute on localhost. 获取运行在本机作业节点的失效转移分片项集合
     * 
     * @return failover items which execute on localhost
     */
    public List<Integer> getLocalFailoverItems() {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return Collections.emptyList();
        }
        return getFailoverItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * Get failover items which crashed on localhost. 获取运行在本机作业服务器的被失效转移的序列号
     * 
     * @return failover items which crashed on localhost  运行在本机作业服务器的被失效转移的序列号
     */
    public List<Integer> getLocalTakeOffItems() {
        List<Integer> shardingItems = shardingService.getLocalShardingItems();
        List<Integer> result = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(each))) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * Remove failover info. 删除作业失效转移信息
     */
    public void removeFailoverInfo() {
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT)) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(Integer.parseInt(each)));
        }
    }
    
    class FailoverLeaderExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            if (JobRegistry.getInstance().isShutdown(jobName) || !needFailover()) { // 判断需要失效转移。再次调用 needFailover()，确保经过分布式锁获取等待过程中，仍然需要失效转移。因为可能多个作业节点调用了该回调，第一个作业节点执行了失效转移，可能第二个作业节点就不需要执行失效转移了。
                return;
            }
            int crashedItem = Integer.parseInt(jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).get(0));  // 获得一个 ${JOB_NAME}/leader/failover/items/${ITEM_ID} 作业分片项
            log.debug("Failover job '{}' begin, crashed item '{}'", jobName, crashedItem);
            jobNodeStorage.fillEphemeralJobNode(FailoverNode.getExecutionFailoverNode(crashedItem), JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());  // 设置这个临时数据节点 ${JOB_NAME}/sharding/${ITEM_ID}/failover 作业分片项 为 当前作业节点
            jobNodeStorage.fillJobNode(FailoverNode.getExecutingFailoverNode(crashedItem), JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId()); // 设置 /sharding/${ITEM_ID}/failovering 作业分片项 为 当前作业节点
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getItemsNode(crashedItem)); // 移除这个 ${JOB_NAME}/leader/failover/items/${ITEM_ID} 作业分片项
            // TODO Instead of using triggerJob, use executor for unified scheduling 不应使用triggerJob, 而是使用executor统一调度
            JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
            if (null != jobScheduleController) {
                jobScheduleController.triggerJob(); // 触发作业执行，实际作业不会立即执行
            }
        }
    }
}
