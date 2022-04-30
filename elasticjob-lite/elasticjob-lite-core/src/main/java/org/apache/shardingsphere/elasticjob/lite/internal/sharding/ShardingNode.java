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

package org.apache.shardingsphere.elasticjob.lite.internal.sharding;

import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderNode;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;

/**
 * Sharding node. 分片节点路径
 */
public final class ShardingNode {
    // 执行状态根节点。/sharding/${ITEM_ID} 目录下以作业分片项序号( ITEM_ID ) 为数据节点路径存储作业分片项的 instance / running / misfire / disable 数据节点信息
    public static final String ROOT = "sharding";
    
    private static final String INSTANCE_APPENDIX = "instance";
    // /sharding/${ITEM_ID}/instance 是临时节点，存储该作业分片项分配到的作业实例主键( JOB_INSTANCE_ID )
    private static final String INSTANCE = ROOT + "/%s/" + INSTANCE_APPENDIX;
    
    private static final String RUNNING_APPENDIX = "running";
    // /sharding/${ITEM_ID}/running 是临时节点，当该作业分片项正在运行，存储空串( "" )；当该作业分片项不在运行，移除该数据节点
    private static final String RUNNING = ROOT + "/%s/" + RUNNING_APPENDIX;
    // /sharding/${ITEM_ID}/misfire 是永久节点，当该作业分片项被错过执行，存储空串( "" )；当该作业分片项重新执行，移除该数据节点
    private static final String MISFIRE = ROOT + "/%s/misfire";
    // /sharding/${ITEM_ID}/disable 是永久节点，当该作业分片项被禁用，存储空串( "" )；当该作业分片项被开启，移除数据节点
    private static final String DISABLED = ROOT + "/%s/disabled";
    // 作业分片项分配。当且仅当作业节点为主节点时，才可以执行作业分片项分配
    private static final String LEADER_ROOT = LeaderNode.ROOT + "/" + ROOT;
    // leader/sharding/necessary 是永久节点，当相同作业有新的作业节点加入或者移除时，存储空串( "" )，标记需要进行作业分片项重新分配；当重新分配完成后，移除该数据节点
    static final String NECESSARY = LEADER_ROOT + "/necessary";
    // leader/sharding/processing 是临时节点，当开始重新分配作业分片项时，存储空串( "" )，标记正在进行重新分配；当重新分配完成后，移除该数据节点
    static final String PROCESSING = LEADER_ROOT + "/processing";
    
    private final JobNodePath jobNodePath;
    
    public ShardingNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * Get the path of instance node.
     *
     * @param item sharding item
     * @return the path of instance node
     */
    public static String getInstanceNode(final int item) {
        return String.format(INSTANCE, item);
    }
    
    /**
     * Get job running node.
     *
     * @param item sharding item
     * @return job running node
     */
    public static String getRunningNode(final int item) {
        return String.format(RUNNING, item);
    }
    
    static String getMisfireNode(final int item) {
        return String.format(MISFIRE, item);
    }
    
    static String getDisabledNode(final int item) {
        return String.format(DISABLED, item);
    }
    
    /**
     * Get item by running item path.
     *
     * @param path running item path
     * @return running item, return null if sharding item is not running
     */
    public Integer getItemByRunningItemPath(final String path) {
        if (!isRunningItemPath(path)) {
            return null;
        }
        return Integer.parseInt(path.substring(jobNodePath.getFullPath(ROOT).length() + 1, path.lastIndexOf(RUNNING_APPENDIX) - 1));
    }
    
    private boolean isRunningItemPath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(ROOT)) && path.endsWith(RUNNING_APPENDIX);
    }
}
