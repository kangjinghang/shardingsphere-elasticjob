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

package org.apache.shardingsphere.elasticjob.lite.internal.failover;

import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderNode;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingNode;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;

/**
 * Failover node. 失效转移节点路径。 为什么 /leader/failover 放在 /leader 目录下，而不独立成为一个根目录？经过确认，作业失效转移涉及到分布式锁，统一存储在 /leader 目录下
 */
public final class FailoverNode {
    
    private static final String FAILOVER = "failover";
    
    private static final String LEADER_ROOT = LeaderNode.ROOT + "/" + FAILOVER;
    
    static final String ITEMS_ROOT = LEADER_ROOT + "/items";
    // /leader/items/${ITEM_ID} 是永久节点，当某台作业节点 CRASH 时，其分配的作业分片项标记需要进行失效转移，存储其分配的作业分片项的 /leader/items/${ITEM_ID} 为空串( "" )；当失效转移标记，移除
    private static final String ITEMS = ITEMS_ROOT + "/%s";
    // /leader/failover/latch 作业失效转移分布式锁，和 /leader/failover/latch 是一致的。
    static final String LATCH = LEADER_ROOT + "/latch";
    // ${JOB_NAME}/sharding/${ITEM_ID}/failover
    private static final String EXECUTION_FAILOVER = ShardingNode.ROOT + "/%s/" + FAILOVER;

    private static final String FAILOVERING = "failovering";
    // 存储 /sharding/${ITEM_ID}/failovering 为空串( "" )，临时节点，需要进行失效转移执行。
    private static final String EXECUTING_FAILOVER = ShardingNode.ROOT + "/%s/" + FAILOVERING;
    
    private final JobNodePath jobNodePath;
    
    public FailoverNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }
    
    static String getItemsNode(final int item) {
        return String.format(ITEMS, item);
    }
    
    static String getExecutionFailoverNode(final int item) {
        return String.format(EXECUTION_FAILOVER, item);
    }

    static String getExecutingFailoverNode(final int item) {
        return String.format(EXECUTING_FAILOVER, item);
    }
    
    /**
     * Get sharding item by execution failover path.
     *
     * @param path failover path
     * @return sharding item, return null if not from failover path
     */
    public Integer getItemByExecutionFailoverPath(final String path) {
        if (!isFailoverPath(path)) {
            return null;
        }
        return Integer.parseInt(path.substring(jobNodePath.getFullPath(ShardingNode.ROOT).length() + 1, path.lastIndexOf(FailoverNode.FAILOVER) - 1));
    }
    
    private boolean isFailoverPath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(ShardingNode.ROOT)) && path.endsWith(FailoverNode.FAILOVER);
    }
}
