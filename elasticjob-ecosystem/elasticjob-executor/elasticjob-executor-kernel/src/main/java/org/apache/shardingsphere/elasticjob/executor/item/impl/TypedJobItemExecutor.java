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
import org.apache.shardingsphere.elasticjob.infra.spi.TypedSPI;

/** Type 类型的作业只需提供类型名称即可，开发者无需实现该作业接口，而是通过外置配置的方式使用。典型代表：Script 类型、HTTP 类型。
 * Typed job item executor.  作业实现远程调用执行器，如 http，feign，不用实现作业类，作业进程是通用的执行节点，不会依赖作业业务
 */
public interface TypedJobItemExecutor extends JobItemExecutor<ElasticJob>, TypedSPI {
}
