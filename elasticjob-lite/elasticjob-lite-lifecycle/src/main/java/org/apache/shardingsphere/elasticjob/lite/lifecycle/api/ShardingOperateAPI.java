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
 * Sharding operate API. 操作分片的 API
 */
public interface ShardingOperateAPI {
    
    /**
     * Disable job sharding item. 禁用作业分片
     * 
     * @param jobName job name 作业名称
     * @param item sharding item 作业分片项
     */
    void disable(String jobName, String item);
    
    /**
     * Enable job sharding item. 启用作业分片
     *
     * @param jobName job name 作业名称
     * @param item sharding item 作业分片项
     */
    void enable(String jobName, String item);
}
