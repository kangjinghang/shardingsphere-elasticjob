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

package org.apache.shardingsphere.elasticjob.lite.internal.listener;

import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobScheduleController;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ExecutionService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;

/**
 * Registry center connection state listener.  连接状态的监听器，连接断开暂停作业。重连后，重新初始化作业，清除分片，重启作业
 */
public final class RegistryCenterConnectionStateListener implements ConnectionStateChangedEventListener {
    
    private final String jobName;
    
    private final ServerService serverService;
    
    private final InstanceService instanceService;
    
    private final ShardingService shardingService;
    
    private final ExecutionService executionService;
    
    public RegistryCenterConnectionStateListener(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
    }
    
    @Override
    public void onStateChanged(final CoordinatorRegistryCenter registryCenter, final State newState) {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return;
        }
        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
        if (State.UNAVAILABLE == newState) { // 暂停作业调度
            jobScheduleController.pauseJob();
        } else if (State.RECONNECTED == newState) {  // Zookeeper 重新连上
            serverService.persistOnline(serverService.isEnableServer(JobRegistry.getInstance().getJobInstance(jobName).getServerIp())); // 持久化作业服务器上线信息
            instanceService.persistOnline(); // 持久化作业运行实例上线相关信息
            executionService.clearRunningInfo(shardingService.getLocalShardingItems()); // 清除本地分配的作业分片项运行中的标记
            jobScheduleController.resumeJob(); // 恢复作业调度
        }
    }
}
