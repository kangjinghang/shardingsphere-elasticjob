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

package org.apache.shardingsphere.elasticjob.lite.internal.election;

import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;

/**
 * Leader path node. 在 leader 目录下一共有三个存储子节点：election：主节点选举；sharding：作业分片项分配；failover：作业失效转移
 */
public final class LeaderNode { // 当前类只是 election：主节点选举相关
    //     // LeaderNode，主节点路径。
    public static final String ROOT = "leader";
    
    private static final String ELECTION_ROOT = ROOT + "/election";
    // /leader/election/instance 是临时节点，当作业集群完成选举后，存储主作业实例主键( JOB_INSTANCE_ID )
    static final String INSTANCE = ELECTION_ROOT + "/instance";
    // /leader/election/latch 主节点选举分布式锁，是 Apache Curator 针对 Zookeeper 实现的分布式锁的一种
    static final String LATCH = ELECTION_ROOT + "/latch";
    
    private final JobNodePath jobNodePath;
    
    LeaderNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }
    
    boolean isLeaderInstancePath(final String path) {
        return jobNodePath.getFullPath(INSTANCE).equals(path);
    }
}
