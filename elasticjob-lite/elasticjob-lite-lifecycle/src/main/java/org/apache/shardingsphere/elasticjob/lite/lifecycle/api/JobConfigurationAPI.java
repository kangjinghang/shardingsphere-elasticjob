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

package org.apache.shardingsphere.elasticjob.lite.lifecycle.api;

import org.apache.shardingsphere.elasticjob.infra.pojo.JobConfigurationPOJO;

/**
 * Job configuration API.
 */
public interface JobConfigurationAPI {
    
    /**
     * get job configuration. 获取作业配置
     *
     * @param jobName job name 作业名称
     * @return job configuration 作业配置对象
     */
    JobConfigurationPOJO getJobConfiguration(String jobName);
    
    /**
     * Update job configuration. 更新作业配置
     *
     * @param jobConfig job configuration 作业配置对象
     */
    void updateJobConfiguration(JobConfigurationPOJO jobConfig);
    
    /**
     * Remove job configuration. 删除作业设置
     *
     * @param jobName job name 作业名称
     */
    void removeJobConfiguration(String jobName);
}
