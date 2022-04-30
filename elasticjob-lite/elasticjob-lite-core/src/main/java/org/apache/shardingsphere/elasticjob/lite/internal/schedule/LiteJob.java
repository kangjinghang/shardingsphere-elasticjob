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

package org.apache.shardingsphere.elasticjob.lite.internal.schedule;

import lombok.Setter;
import org.apache.shardingsphere.elasticjob.executor.ElasticJobExecutor;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

/**
 * Lite job class. 实现了 quartz 的 job 接口，作为 quartz 与 elastic-job 作业的桥接
 */
@Setter
public final class LiteJob implements Job {
    // 用 quartz 的参数机制注入
    private ElasticJobExecutor jobExecutor;
    
    @Override
    public void execute(final JobExecutionContext context) {
        jobExecutor.execute();
    }
    
}
