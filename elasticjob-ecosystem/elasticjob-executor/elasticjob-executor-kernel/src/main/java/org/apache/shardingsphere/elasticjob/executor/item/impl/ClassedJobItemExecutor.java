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

package org.apache.shardingsphere.elasticjob.executor.item.impl;

import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.executor.item.JobItemExecutor;

/**
 * Classed job item executor.  实现执行器/作业
 * Class 类型的作业由开发者直接使用，需要由开发者实现该作业接口实现业务逻辑。典型代表：Simple 类型、Dataflow 类型。
 * @param <T> type of ElasticJob
 */
public interface ClassedJobItemExecutor<T extends ElasticJob> extends JobItemExecutor<T> {
    
    /**
     * Get elastic job class.
     * 
     * @return elastic job class
     */
    Class<T> getElasticJobClass();
}
