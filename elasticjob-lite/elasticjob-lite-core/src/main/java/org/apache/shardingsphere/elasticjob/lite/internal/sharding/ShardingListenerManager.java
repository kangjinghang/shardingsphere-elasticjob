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

import org.apache.shardingsphere.elasticjob.infra.pojo.JobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationNode;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerNode;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

/**
 * Sharding listener manager. 继承作业注册中心的监听器管理者的抽象类，内部管理了 ShardingTotalCountChangedJobListener / ListenServersChangedJobListener 两个作业注册中心监听器
 */
public final class ShardingListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final ConfigurationNode configNode;
    
    private final InstanceNode instanceNode;
    
    private final ServerNode serverNode;
    
    private final ShardingService shardingService;
    
    private final JobNodePath jobNodePath;
    
    private final ConfigurationService configService;
    
    public ShardingListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
        serverNode = new ServerNode(jobName);
        shardingService = new ShardingService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
        configService = new ConfigurationService(regCenter, jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new ShardingTotalCountChangedJobListener());
        addDataListener(new ListenServersChangedJobListener());
    }
    // 监听作业配置分片数变更
    class ShardingTotalCountChangedJobListener implements DataChangedEventListener {
        
        @Override
        public void onChange(final DataChangedEvent event) {
            if (configNode.isConfigPath(event.getKey()) && 0 != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {
                int newShardingTotalCount = YamlEngine.unmarshal(event.getValue(), JobConfigurationPOJO.class).toJobConfiguration().getShardingTotalCount();
                if (newShardingTotalCount != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {
                    shardingService.setReshardingFlag(); // 设置需要重新分片的标记
                    JobRegistry.getInstance().setCurrentShardingTotalCount(jobName, newShardingTotalCount); // 设置当前分片总数
                }
            }
        }
    }
    // 监听 server znode 和 instance znode，对于一个作业，server 可有对应多个 instance，因此 server 的上下线，同时接收到 server 和 instance 事件
    class ListenServersChangedJobListener implements DataChangedEventListener {
        
        @Override
        public void onChange(final DataChangedEvent event) { // #isServerChange(...) 服务器被开启或禁用，#isInstanceChange(...) 作业节点新增或者移除
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isInstanceChange(event.getType(), event.getKey()) || isServerChange(event.getKey())) && !(isStaticSharding() && hasShardingInfo())) {
                shardingService.setReshardingFlag();
            }
        }
        
        private boolean isStaticSharding() {
            return configService.load(true).isStaticSharding();
        }
        
        private boolean hasShardingInfo() {
            return !JobRegistry.getInstance().getRegCenter(jobName).getChildrenKeys(jobNodePath.getShardingNodePath()).isEmpty();
        }
        // 作业节点新增或者移除
        private boolean isInstanceChange(final Type eventType, final String path) {
            return instanceNode.isInstancePath(path) && Type.UPDATED != eventType;
        }
        // 服务器被开启或禁用
        private boolean isServerChange(final String path) {
            return serverNode.isServerPath(path);
        }
    }
}
