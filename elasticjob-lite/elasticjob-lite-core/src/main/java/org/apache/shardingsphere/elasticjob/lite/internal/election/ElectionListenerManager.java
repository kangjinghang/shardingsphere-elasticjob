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

import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerNode;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerStatus;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.util.Objects;

/**
 * Election listener manager. 继承作业注册中心的监听器管理者的抽象类
 */
public final class ElectionListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final LeaderNode leaderNode;
    
    private final ServerNode serverNode;
    
    private final LeaderService leaderService;
    
    private final ServerService serverService;
    
    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        leaderNode = new LeaderNode(jobName);
        serverNode = new ServerNode(jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new LeaderElectionJobListener());
        addDataListener(new LeaderAbdicationJobListener());
    }
    // 节点数据发生变化时，重新选举
    class LeaderElectionJobListener implements DataChangedEventListener {
        // 符合重新选举主节点分成两种情况：1.主动选举 #isActiveElection(...) 2.被动选举 #isPassiveElection(...)
        @Override
        public void onChange(final DataChangedEvent event) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isActiveElection(event.getKey(), event.getValue()) || isPassiveElection(event.getKey(), event.getType()))) {
                leaderService.electLeader();
            }
        }
        
        private boolean isActiveElection(final String path, final String data) {
            return !leaderService.hasLeader() && isLocalServerEnabled(path, data); // 不存在主节点 && 开启作业，不再禁用
        }
        
        private boolean isPassiveElection(final String path, final Type eventType) {
            JobInstance jobInstance = JobRegistry.getInstance().getJobInstance(jobName);
            return !Objects.isNull(jobInstance) && isLeaderCrashed(path, eventType) && serverService.isAvailableServer(jobInstance.getServerIp()); // 主节点 Crashed && 当前节点正在运行中（未挂掉）
        }
        // 主节点是否 Crashed，必须主节点被删除后才可以重新进行选举，实际主作业节点正常退出也符合被动选举条件
        private boolean isLeaderCrashed(final String path, final Type eventType) {
            return leaderNode.isLeaderInstancePath(path) && Type.DELETED == eventType;
        }
        // 它不是通过作业节点是否处于开启状态，而是该数据不是将作业节点更新成关闭状态。举个例子：作业节点处于禁用状态，使用运维平台设置作业节点开启，会进行主节点选举；作业节点处于开启状态，使用运维平台设置作业节点禁用，不会进行主节点选举
        private boolean isLocalServerEnabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
        }
    }
    // 作业被禁用。被禁用的作业注册作业启动信息时即使进行了主节点选举，也会被该监听器处理，移除该选举的主节点
    class LeaderAbdicationJobListener implements DataChangedEventListener {
        
        @Override
        public void onChange(final DataChangedEvent event) {
            if (leaderService.isLeader() && isLocalServerDisabled(event.getKey(), event.getValue())) {
                leaderService.removeLeader();
            }
        }
        // 当前作业服务器被禁用
        private boolean isLocalServerDisabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
        }
    }
}
