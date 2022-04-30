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

/**
 * Job operate API.
 */
public interface JobOperateAPI {
    
    /**
     * Trigger job to run at once. 触发作业执行，作业在不与当前运行中作业冲突的情况下才会触发执行，并在启动后自动清理此标记。
     *
     * <p>Job will not start until it does not conflict with the last running job, and this tag will be automatically cleaned up after it starts.</p>
     *
     * @param jobName job name 作业名称
     */
    void trigger(String jobName);
    
    /**
     * Disable job. 禁用作业，禁用作业将会导致分布式的其他作业触发重新分片。
     *
     * <p>Will cause resharding.</p>
     *
     * @param jobName job name 作业名称
     * @param serverIp server IP address 作业服务器 IP 地址
     */
    void disable(String jobName, String serverIp);
    
    /**
     * Enable job. 启用作业
     * 
     * @param jobName job name 作业名称
     * @param serverIp server IP address 作业服务器 IP 地址
     */
    void enable(String jobName, String serverIp);
    
    /**
     * Shutdown Job. 停止调度作业
     *
     * @param jobName job name 作业名称
     * @param serverIp server IP address 作业服务器 IP 地址
     */
    void shutdown(String jobName, String serverIp);
    
    /**
     * Remove job. 删除作业
     * 
     * @param jobName job name 作业名称
     * @param serverIp server IP address 作业服务器 IP 地址
     */
    void remove(String jobName, String serverIp);
}
