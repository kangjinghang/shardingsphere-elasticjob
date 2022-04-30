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

package org.apache.shardingsphere.elasticjob.infra.handler.sharding;

import org.apache.shardingsphere.elasticjob.infra.spi.TypedSPI;

import java.util.List;
import java.util.Map;

/**
 * Job sharding strategy. 作业分片策略接口
 */
public interface JobShardingStrategy extends TypedSPI {
    
    /**
     * Sharding job. 作业分片
     * 
     * @param jobInstances all job instances which participate in sharding 所有参与分片的单元列表
     * @param jobName job name 作业名称
     * @param shardingTotalCount sharding total count 分片总数
     * @return sharding result 分片结果
     */
    Map<JobInstance, List<Integer>> sharding(List<JobInstance> jobInstances, String jobName, int shardingTotalCount);
}
