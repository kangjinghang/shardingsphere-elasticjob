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

package org.apache.shardingsphere.elasticjob.reg.base;

import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.util.List;

/**
 * Coordinator registry center.  znode 节点存取。用于协调分布式服务的注册中心，定义了持久节点、临时节点、持久顺序节点、临时顺序节点等目录服务接口方法，隐性的要求提供事务、分布式锁、数据订阅等特性。
 */
public interface CoordinatorRegistryCenter extends RegistryCenter {
    
    /**
     * Get value from registry center directly. 直接从 Zookeeper 获取
     * 
     * @param key key
     * @return value
     */
    String getDirectly(String key);
    
    /**
     * Get children keys. 获取子节点名称集合(降序)
     * 
     * @param key key
     * @return children keys
     */
    List<String> getChildrenKeys(String key);
    
    /**
     * Get children number. 获取子节点数量
     *
     * @param key key
     * @return children number
     */
    int getNumChildren(String key);
    
    /**
     * Persist ephemeral data. 存储临时节点数据。节点类型无法变更，因此如果数据已存在，需要先进行删除
     * 
     * @param key key
     * @param value value
     */
    void persistEphemeral(String key, String value);
    
    /**
     * Persist sequential data. 存储顺序注册数据
     *
     * @param key key
     * @param value value
     * @return value which include 10 digital
     */
    String persistSequential(String key, String value);
    
    /**
     * Persist ephemeral sequential data.
     * 
     * @param key key
     */
    void persistEphemeralSequential(String key);
    
    /**
     * Add data to cache.
     * 
     * @param cachePath cache path
     */
    void addCacheData(String cachePath);
    
    /**
     * Evict data from cache. 关闭作业缓存
     *
     * @param cachePath cache path
     */
    void evictCacheData(String cachePath);
    
    /**
     * Get raw cache object of registry center.
     * 
     * @param cachePath cache path
     * @return raw cache object of registry center
     */
    Object getRawCache(String cachePath);
    
    /**
     * Execute in leader. 在主节点执行操作
     *
     * @param key key 分布式锁使用的节点，例如：leader/election/latch
     * @param callback callback of leader 执行操作的回调
     */
    void executeInLeader(String key, LeaderExecutionCallback callback);
    
    /**
     * Watch changes of a key.
     *
     * @param key key to be watched
     * @param listener data listener
     */
    void watch(String key, DataChangedEventListener listener);
    
    /**
     * Add connection state changed event listener to registry center.
     *
     * @param listener connection state changed event listener
     */
    void addConnectionStateChangedEventListener(ConnectionStateChangedEventListener listener);
    
    /**
     * Execute oprations in transaction.
     *
     * @param transactionOperations operations
     * @throws Exception exception
     */
    void executeInTransaction(List<TransactionOperation> transactionOperations) throws Exception;
}
