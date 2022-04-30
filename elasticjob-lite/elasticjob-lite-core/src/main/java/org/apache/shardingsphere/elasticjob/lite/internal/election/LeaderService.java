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

package org.apache.shardingsphere.elasticjob.lite.internal.election;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;

/**
 * Leader service.
 */
@Slf4j
public final class LeaderService {
    
    private final String jobName;
    
    private final ServerService serverService;
    
    private final JobNodeStorage jobNodeStorage;
    
    public LeaderService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    /**
     * Elect leader. 选举主节点
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        jobNodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.debug("Leader election completed.");
    }
    
    /**
     * Judge current server is leader or not. 判断当前节点是否是主节点
     * 
     * <p>
     * If leader is electing, this method will block until leader elected success. 如果主节点正在选举中而导致取不到主节点， 则阻塞至主节点选举完成再返回
     * </p>
     * 
     * @return current server is leader or not
     */
    public boolean isLeaderUntilBlock() {
        while (!hasLeader() && serverService.hasAvailableServers()) { // 不存在主节点 && 有可用的服务器节点
            log.info("Leader is electing, waiting for {} ms", 100);
            BlockUtils.waitingShortTime();
            if (!JobRegistry.getInstance().isShutdown(jobName) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getServerIp())) {
                electLeader();
            }
        }
        return isLeader();  // 返回当前节点是否是主节点
    }
    
    /**
     * Judge current server is leader or not.
     *
     * @return current server is leader or not
     */
    public boolean isLeader() {
        return !JobRegistry.getInstance().isShutdown(jobName) && JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }
    
    /**
     * Judge has leader or not in current time. 判断是否已经有主节点
     * 
     * @return has leader or not in current time 是否已经有主节点
     */
    public boolean hasLeader() {
        return jobNodeStorage.isJobNodeExisted(LeaderNode.INSTANCE);
    }
    
    /**
     * Remove leader and trigger leader election. 删除主节点供重新选举。删除主节点时机：1.主节点进程正常关闭时 2.主节点进程 CRASHED 时 3.作业被禁用时 4.主节点进程远程关闭
     */
    public void removeLeader() { // ${JOB_NAME}/leader/electron/instance 是临时节点，主节点进程 CRASHED 后，超过最大会话时间，Zookeeper 自动进行删除，触发重新选举逻辑
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }
    
    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            if (!hasLeader()) {  // 当前无主节点。为什么要调用 #hasLeader() 呢？LeaderLatch 只保证同一时间有且仅有一个工作节点，但是在获得分布式锁的工作节点结束逻辑后，第二个工作节点会开始逻辑，如果不判断当前是否有主节点，原来的主节点会被覆盖。
                jobNodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
            }
        }
    }
}
