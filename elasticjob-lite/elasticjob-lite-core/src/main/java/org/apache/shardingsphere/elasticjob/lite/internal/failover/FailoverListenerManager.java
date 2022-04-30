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

import org.apache.shardingsphere.elasticjob.infra.pojo.JobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationNode;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.util.List;

/**
 * Failover listener manager.
 */
public final class FailoverListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final ConfigurationService configService;
    
    private final ShardingService shardingService;
    
    private final FailoverService failoverService;
    
    private final ConfigurationNode configNode;
    
    private final InstanceNode instanceNode;
    
    public FailoverListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new JobCrashedJobListener());
        addDataListener(new FailoverSettingsChangedJobListener());
    }
    
    private boolean isFailoverEnabled() {
        return configService.load(true).isFailover();
    }
    // 监听运行实例znode /instances/{instanceId} 删除事件，即运行实例【下线】（虽然名为【崩溃】）事件，本节点下线不处理
    class JobCrashedJobListener implements DataChangedEventListener {

        @Override
        public void onChange(final DataChangedEvent event) { // 通过判断 /${JOB_NAME}/instances/${INSTANCE_ID} 被移除，执行作业失效转移逻辑。
            if (!JobRegistry.getInstance().isShutdown(jobName) && isFailoverEnabled() && Type.DELETED == event.getType() && instanceNode.isInstancePath(event.getKey())) {
                String jobInstanceId = event.getKey().substring(instanceNode.getInstanceFullPath().length() + 1); // 下线实例 instanceId
                if (jobInstanceId.equals(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())) { // 本节点不处理
                    return;
                }
                List<Integer> failoverItems = failoverService.getFailoveringItems(jobInstanceId); // 优先调用，获取【下线实例抢到】的失效转移分片（分配给它的作业分片项已经执行完成，抢的别人的？）， /${JOB_NAME}/sharding/${ITEM_ID}/failover 下的作业分片项
                if (!failoverItems.isEmpty()) { // 获取下线实例的失效转移中的分片
                    for (int each : failoverItems) {
                        failoverService.setCrashedFailoverFlagDirectly(each);
                        failoverService.failoverIfNecessary(); // 主节点回调处理
                    }
                } else { // 若 failoverItems 为空，获得关闭作业节点( ${JOB_INSTANCE_ID} )对应的 /${JOB_NAME}/sharding/${ITEM_ID}/instance 作业分片项
                    for (int each : shardingService.getCrashedShardingItems(jobInstanceId)) { // /${JOB_NAME}/sharding/${ITEM_ID}/instance
                        failoverService.setCrashedFailoverFlag(each);
                        failoverService.failoverIfNecessary();
                    }
                }
            }
        }
    }
    //  监听作业失效转移功能关闭
    class FailoverSettingsChangedJobListener implements DataChangedEventListener {
        
        @Override
        public void onChange(final DataChangedEvent event) {
            if (configNode.isConfigPath(event.getKey()) && Type.UPDATED == event.getType() && !YamlEngine.unmarshal(event.getValue(), JobConfigurationPOJO.class).toJobConfiguration().isFailover()) {
                failoverService.removeFailoverInfo(); // 关闭失效转移功能
            }
        }
    }
}
