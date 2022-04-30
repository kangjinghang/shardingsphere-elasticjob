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

package org.apache.shardingsphere.elasticjob.infra.handler.sharding.impl;

import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sharding strategy which for round robin by name job. 根据作业名的哈希值对作业节点列表进行轮转的分片策略。这里的轮转怎么定义呢？如果有 3 台作业节点，顺序为 [0, 1, 2]，如果作业名的哈希值根据作业分片总数取模为 1, 作业节点顺序变为 [1, 2, 0]
 */ // 分片的目的，是将作业的负载合理的分配到不同的作业节点上，要避免分片策略总是让固定的作业节点负载特别大，其它工作节点负载特别小。
public final class RoundRobinByNameJobShardingStrategy implements JobShardingStrategy {
    
    private final AverageAllocationJobShardingStrategy averageAllocationJobShardingStrategy = new AverageAllocationJobShardingStrategy();
    
    @Override
    public Map<JobInstance, List<Integer>> sharding(final List<JobInstance> jobInstances, final String jobName, final int shardingTotalCount) {
        return averageAllocationJobShardingStrategy.sharding(rotateServerList(jobInstances, jobName), jobName, shardingTotalCount);
    }
    
    private List<JobInstance> rotateServerList(final List<JobInstance> shardingUnits, final String jobName) {
        int shardingUnitsSize = shardingUnits.size();
        int offset = Math.abs(jobName.hashCode()) % shardingUnitsSize; // 轮转开始位置
        if (0 == offset) {
            return shardingUnits;
        }
        List<JobInstance> result = new ArrayList<>(shardingUnitsSize);
        for (int i = 0; i < shardingUnitsSize; i++) {
            int index = (i + offset) % shardingUnitsSize;
            result.add(shardingUnits.get(index));
        }
        return result;
    }
    
    @Override
    public String getType() {
        return "ROUND_ROBIN";
    }
}
