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

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategy;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategyFactory;
import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Sharding service.
 */
@Slf4j
public final class ShardingService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final LeaderService leaderService;
    
    private final ConfigurationService configService;
    
    private final InstanceService instanceService;
    
    private final InstanceNode instanceNode;
    
    private final ServerService serverService;
    
    private final ExecutionService executionService;

    private final JobNodePath jobNodePath;
    
    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        instanceNode = new InstanceNode(jobName);
        serverService = new ServerService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * Set resharding flag. 设置需要重新分片的标记。当作业满足分片条件时，不会立即进行作业分片分配，而是设置需要重新进行分片的标记，等到作业分片获取时，判断有该标记后执行作业分配
     */ // 设置需要重新进行分片有 3 种情况：1.作业分片总数( JobCoreConfiguration.shardingTotalCount )变化时 2.服务器变化时 3.自我诊断修复时
    public void setReshardingFlag() { // /${JOB_NAME}/leader/sharding/necessary，该 Zookeeper 数据节点是永久节点，存储空串( "" )
        if (!leaderService.isLeaderUntilBlock()) {
            return;
        }
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY);
    }
    
    /**
     * Judge is need resharding or not. 判断是否需要重分片
     * 
     * @return is need resharding or not 是否需要重分片
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY);
    }
    
    /**
     * Sharding if necessary. 如果需要分片且当前节点为主节点， 则作业分片
     * 作业分片项的分配过程：【主节点】执行作业分片项分配。【非主节点】等待作业分片项分配完成。
     * <p>
     * Sharding if current job server is leader server;
     * Do not sharding if no available job server.  如果当前无可用节点则不分片
     * </p>
     */
    public void shardingIfNecessary() {
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances(); // 获取在线的运行示例
        if (!isNeedSharding() || availableJobInstances.isEmpty()) { // 判断是否需要重新分片
            return;
        } // 【非主节点】等待 作业分片项分配完成
        if (!leaderService.isLeaderUntilBlock()) { // 判断是否为【主节点】，分片主节点负责
            blockUntilShardingCompleted(); // 如果当前节点不是主节点，那么就阻塞等待分片完成
            return;
        }  // 走到这里，说明当前节点是【主节点】。可以进行作业分片项分配了
        waitingOtherShardingItemCompleted(); // 等待作业未在运行中状态变更。作业是否在运行中需要 LiteJobConfiguration.monitorExecution = true
        JobConfiguration jobConfig = configService.load(false); // 从注册中心获取作业配置( 非缓存 )，避免主节点本地作业配置可能非最新的，主要目的是获得作业分片总数( shardingTotalCount )。
        int shardingTotalCount = jobConfig.getShardingTotalCount();
        log.debug("Job '{}' sharding begin.", jobName); // 设置 作业正在重分片的标记
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, ""); // 设置【作业正在重分片】的标记 /${JOB_NAME}/leader/sharding/processing。该 Zookeeper 数据节点是临时节点，存储空串( "" )，仅用于标记作业正在重分片，无特别业务逻辑。
        resetShardingInfo(shardingTotalCount); // 重置 作业分片项信息。 重写分片的 znode，去掉/sharding/{itemNum}/instance 分片分配，剪掉之前可能多出的分片 znode
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(jobConfig.getJobShardingStrategyType()); // 计算每个节点分配的作业分片项
        jobNodeStorage.executeInTransaction(getShardingResultTransactionOperations(jobShardingStrategy.sharding(availableJobInstances, jobName, shardingTotalCount))); // 在【事务中】设置 作业分片项信息，设置临时数据节点 /${JOB_NAME}/sharding/${ITEM_ID}/instance 为分配的作业节点的作业实例主键( jobInstanceId )
        log.debug("Job '{}' sharding complete.", jobName);
    }
    // 阻塞等待分片完成。为什么上面判断了一次，这里又判断一次？主节点作业分片项分配过程中，不排除自己挂掉了，此时【非主节点】若选举成主节点，无需继续等待，当然也不能等待，因为已经没节点在执行作业分片项分配，所有节点都会卡在这里。
    private void blockUntilShardingCompleted() { // 当前作业节点不为【主节点】 && 存在作业需要重分片的标记 && 存在作业正在重分片的标记
        while (!leaderService.isLeaderUntilBlock() && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY) || jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    } // 当 【作业需要重分片】的标记、【作业正在重分片】的标记 都不存在时，意味着作业分片项分配已经完成
    
    private void waitingOtherShardingItemCompleted() {
        while (executionService.hasRunningItems()) {
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    // 重置作业分片信息
    private void resetShardingInfo(final int shardingTotalCount) {
        for (int i = 0; i < shardingTotalCount; i++) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i));  // 移除 /${JOB_NAME}/sharding/${ITEM_ID}/instance
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i); // 创建 /${JOB_NAME}/sharding/${ITEM_ID}
        }
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size(); // 移除 多余的作业分片项，就是去掉【之前可能多出】的分片 znode
        if (actualShardingTotalCount > shardingTotalCount) { // 原来的分片数量更大，删除之前多出的分片
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i); // 移除 /${JOB_NAME}/sharding/${ITEM_ID}
            }
        }
    }
    
    private List<TransactionOperation> getShardingResultTransactionOperations(final Map<JobInstance, List<Integer>> shardingResults) {
        List<TransactionOperation> result = new ArrayList<>(shardingResults.size() + 2);
        for (Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) { // 设置 每个节点分配的作业分片项
            for (int shardingItem : entry.getValue()) { // 每个分片项
                String key = jobNodePath.getFullPath(ShardingNode.getInstanceNode(shardingItem));
                String value = new String(entry.getKey().getJobInstanceId().getBytes(), StandardCharsets.UTF_8);
                result.add(TransactionOperation.opAdd(key, value));
            }
        }
        result.add(TransactionOperation.opDelete(jobNodePath.getFullPath(ShardingNode.NECESSARY))); // 移除 【作业需要重分片】的标记、【作业正在重分片】的标记
        result.add(TransactionOperation.opDelete(jobNodePath.getFullPath(ShardingNode.PROCESSING)));
        return result;
    }
    
    /**
     * Get sharding items. 获取作业运行实例的分片项集合
     *
     * @param jobInstanceId job instance ID 作业运行实例主键
     * @return sharding items 作业运行实例的分片项集合
     */
    public List<Integer> getShardingItems(final String jobInstanceId) {
        JobInstance jobInstance = YamlEngine.unmarshal(jobNodeStorage.getJobNodeData(instanceNode.getInstancePath(jobInstanceId)), JobInstance.class);
        if (!serverService.isAvailableServer(jobInstance.getServerIp())) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (jobInstance.getJobInstanceId().equals(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) { // /${JOB_NAME}/sharding/${ITEM_ID}/instance
                result.add(i);
            }
        }
        return result;
    }

    /**
     * Get crashed sharding items.
     *
     * @param jobInstanceId crashed job instance ID
     * @return crashed sharding items
     */
    public List<Integer> getCrashedShardingItems(final String jobInstanceId) {
        String serverIp = jobInstanceId.substring(0, jobInstanceId.indexOf(JobInstance.DELIMITER));
        if (!serverService.isEnableServer(serverIp)) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (jobInstanceId.equals(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                result.add(i);
            }
        }
        return result;
    }
    
    /**
     * Get sharding items from localhost job server. 获取运行在本作业实例的分片项集合
     * 
     * @return sharding items from localhost job server 运行在本作业实例的分片项集合
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getServerIp())) {
            return Collections.emptyList();
        }
        return getShardingItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * Query has sharding info in offline servers or not. 查询是否存在不在运行状态并且含有分片节点的作业服务器
     * 
     * @return has sharding info in offline servers or not 是否存在不在运行状态并且含有分片节点的作业服务器
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT); // 所有在线作业服务器
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }
    
}
