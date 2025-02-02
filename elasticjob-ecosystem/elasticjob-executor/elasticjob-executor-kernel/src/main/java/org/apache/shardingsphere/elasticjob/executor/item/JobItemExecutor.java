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

package org.apache.shardingsphere.elasticjob.executor.item;

import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.executor.JobFacade;

/**
 * Job item executor. 真正的作业执行器，两类作业，Typed/Classed，Typed 类型作业实现远程调用执行器，如 http，feign，不用实现作业类，作业进程是通用的执行节点，不会依赖作业业务；Classed 实现执行器/作业
 * 
 * @param <T> type of ElasticJob
 */
public interface JobItemExecutor<T extends ElasticJob> {
    
    /**
     * Process job item.
     * 
     * @param elasticJob elastic job
     * @param jobConfig job configuration
     * @param jobFacade job facade
     * @param shardingContext sharding context
     */
    void process(T elasticJob, JobConfiguration jobConfig, JobFacade jobFacade, ShardingContext shardingContext);
}
