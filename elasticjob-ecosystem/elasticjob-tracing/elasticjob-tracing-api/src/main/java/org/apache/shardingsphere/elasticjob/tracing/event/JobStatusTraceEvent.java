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

package org.apache.shardingsphere.elasticjob.tracing.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

/**
 * Job status trace event. 作业状态追踪事件
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public final class JobStatusTraceEvent implements JobEvent {
    // 主键
    private String id = UUID.randomUUID().toString();
    // 作业名称
    private final String jobName;
    // 原作业任务ID
    @Setter
    private String originalTaskId = "";
    // 作业任务ID。来自 {@link com.dangdang.ddframe.job.executor.ShardingContexts#taskId}
    private final String taskId;
    // 执行作业服务器的名字。Elastic-Job-Lite，作业节点的 IP 地址。Elastic-Job-Cloud，Mesos 执行机主键
    private final String slaveId;
    // 任务来源
    private final Source source;
    // 任务执行类型
    private final String executionType;
    // 作业分片项。多个分片项以逗号分隔
    private final String shardingItems;
    // 任务执行状态
    private final State state;
    // 相关信息
    private final String message;
    // 记录创建时间
    private Date creationTime = new Date();
    // Elastic-Job-Lite 使用 TASK_STAGING、TASK_RUNNING、TASK_FINISHED、TASK_ERROR 四种执行状态
    public enum State {
        TASK_STAGING, TASK_RUNNING, TASK_FINISHED, TASK_KILLED, TASK_LOST, TASK_FAILED, TASK_ERROR, TASK_DROPPED, TASK_GONE, TASK_GONE_BY_OPERATOR, TASK_UNREACHABLE, TASK_UNKNOWN
    }
    
    public enum Source {
        CLOUD_SCHEDULER, CLOUD_EXECUTOR, LITE_EXECUTOR
    }
}
