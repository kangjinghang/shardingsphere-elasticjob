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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sharding strategy which for average by sharding item. 基于平均分配算法的分片策略。Elastic-Job-Lite 默认的作业分片策略
 * 如何实现主备？通过作业配置设置总分片数为 1 ( JobCoreConfiguration.shardingTotalCount = 1 )，只有一个作业分片能够分配到作业分片项，从而达到一主N备。
 * <p>
 * If the job server number and sharding count cannot be divided, 
 * the redundant sharding item that cannot be divided will be added to the server with small sequence number in turn.
 * 
 * For example:
 * 
 * 1. If there are 3 job servers and the total sharding count is 9, each job server is divided into: 1=[0,1,2], 2=[3,4,5], 3=[6,7,8];
 * 2. If there are 3 job servers and the total sharding count is 8, each job server is divided into: 1=[0,1,6], 2=[2,3,7], 3=[4,5];
 * 3. If there are 3 job servers and the total sharding count is 10, each job server is divided into: 1=[0,1,2,9], 2=[3,4,5], 3=[6,7,8].
 * </p>
 */
public final class AverageAllocationJobShardingStrategy implements JobShardingStrategy {
    
    @Override
    public Map<JobInstance, List<Integer>> sharding(final List<JobInstance> jobInstances, final String jobName, final int shardingTotalCount) {
        if (jobInstances.isEmpty()) { // 不存在 作业运行实例
            return Collections.emptyMap();
        }
        Map<JobInstance, List<Integer>> result = shardingAliquot(jobInstances, shardingTotalCount); // 分配能被整除的部分
        addAliquant(jobInstances, shardingTotalCount, result); // 分配不能被整除的部分
        return result;
    }
    // 如果有 3 台作业节点，分成 8 片，被整除的部分是前 6 片 [0, 1, 2, 3, 4, 5]，调用该方法结果：1=[0,1], 2=[2,3], 3=[4,5]
    private Map<JobInstance, List<Integer>> shardingAliquot(final List<JobInstance> shardingUnits, final int shardingTotalCount) {
        Map<JobInstance, List<Integer>> result = new LinkedHashMap<>(shardingUnits.size(), 1);
        int itemCountPerSharding = shardingTotalCount / shardingUnits.size();  // 每个作业运行实例分配的平均分片数
        int count = 0;
        for (JobInstance each : shardingUnits) {
            List<Integer> shardingItems = new ArrayList<>(itemCountPerSharding + 1);
            for (int i = count * itemCountPerSharding; i < (count + 1) * itemCountPerSharding; i++) {
                shardingItems.add(i);
            }
            result.put(each, shardingItems);
            count++;
        }
        return result;
    }
    // 分配能不被整除的部分。继续上面的例子。不能被整除的部分是后 2 片 [6, 7]，调用该方法结果：1=[0,1] + [6], 2=[2,3] + [7], 3=[4,5]
    private void addAliquant(final List<JobInstance> shardingUnits, final int shardingTotalCount, final Map<JobInstance, List<Integer>> shardingResults) {
        int aliquant = shardingTotalCount % shardingUnits.size(); // 余数
        int count = 0;
        for (Map.Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
            if (count < aliquant) {
                entry.getValue().add(shardingTotalCount / shardingUnits.size() * shardingUnits.size() + count);
            }
            count++;
        }
    }
    
    @Override
    public String getType() {
        return "AVG_ALLOCATION";
    }
}
