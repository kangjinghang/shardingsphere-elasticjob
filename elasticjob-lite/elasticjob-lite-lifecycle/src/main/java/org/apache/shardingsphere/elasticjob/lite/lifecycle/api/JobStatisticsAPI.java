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

import org.apache.shardingsphere.elasticjob.lite.lifecycle.domain.JobBriefInfo;

import java.util.Collection;

/**
 * Job statistics API. 作业统计 API
 */
public interface JobStatisticsAPI {
    
    /**
     * Get jobs total count. 获取作业总数
     *
     * @return jobs total count. 作业总数
     */
    int getJobsTotalCount();
    
    /**
     * Get all jobs brief info. 获取所有作业简明信息
     *
     * @return all jobs brief info. 作业简明信息集合
     */
    Collection<JobBriefInfo> getAllJobsBriefInfo();
    
    /**
     * Get job brief info. 获取作业简明信息
     *
     * @param jobName job name 作业名称
     * @return job brief info 作业简明信息
     */
    JobBriefInfo getJobBriefInfo(String jobName);
    
    /**
     * Get jobs brief info. 获取该 IP 下所有作业简明信息
     *
     * @param ip server IP address 服务器 IP
     * @return jobs brief info 作业简明信息集合
     */
    Collection<JobBriefInfo> getJobsBriefInfo(String ip);
}
