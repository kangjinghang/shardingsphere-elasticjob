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

package org.apache.shardingsphere.elasticjob.infra.listener;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Sharding contexts. 分片上下文集合
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class ShardingContexts implements Serializable {
    
    private static final long serialVersionUID = -4585977349142082152L;
    // 作业任务ID
    private final String taskId;
    // 作业名称
    private final String jobName;
    // 分片总数
    private final int shardingTotalCount;
    // 作业自定义参数，可以配置多个相同的作业, 但是用不同的参数作为不同的调度实例
    private final String jobParameter;
    // 分配于本作业实例的分片项和参数的Map
    private final Map<Integer, String> shardingItemParameters;
    // 作业事件采样统计数
    private int jobEventSamplingCount;
    // 当前作业事件采样统计数，在 Elastic-Job-Lite 暂未还使用，在 Elastic-Job-Cloud 使用。
    @Setter
    private int currentJobEventSamplingCount;
    // 是否允许可以发送作业事件，在 Elastic-Job-Lite 暂未还使用，在 Elastic-Job-Cloud 使用。
    @Setter
    private boolean allowSendJobEvent = true;
    
    public ShardingContexts(final String taskId, final String jobName, final int shardingTotalCount, final String jobParameter,
                            final Map<Integer, String> shardingItemParameters, final int jobEventSamplingCount) {
        this.taskId = taskId;
        this.jobName = jobName;
        this.shardingTotalCount = shardingTotalCount;
        this.jobParameter = jobParameter;
        this.shardingItemParameters = shardingItemParameters;
        this.jobEventSamplingCount = jobEventSamplingCount;
    }
    
    /**
     * Create sharding context.
     * 
     * @param shardingItem sharding item
     * @return sharding context
     */
    public ShardingContext createShardingContext(final int shardingItem) {
        return new ShardingContext(jobName, taskId, shardingTotalCount, jobParameter, shardingItem, shardingItemParameters.get(shardingItem));
    }
}
