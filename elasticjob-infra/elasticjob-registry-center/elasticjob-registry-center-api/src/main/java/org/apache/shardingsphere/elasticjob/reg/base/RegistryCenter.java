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

/**
 * Registry center. 注册中心，定义了简单的增删改查注册数据和查询时间的接口方法
 */
public interface RegistryCenter {
    
    /**
     * Initialize registry center.
     */
    void init();
    
    /**
     * Close registry center. 关闭
     */
    void close();
    
    /**
     * Get value. 获得数据
     * 
     * @param key key
     * @return value
     */
    String get(String key);
    
    /**
     * Judge node is exist or not. 判断节点是否存在
     * 
     * @param key key
     * @return node is exist or not
     */
    boolean isExisted(String key);
    
    /**
     * Persist data. 存储持久节点数据。逻辑等价于 insertOrUpdate 操作
     * 
     * @param key key
     * @param value value
     */
    void persist(String key, String value);
    
    /**
     * Update data. 使用事务校验键( key )存在才进行更新
     * 
     * @param key key
     * @param value value
     */
    void update(String key, String value);
    
    /**
     * Remove data. 移除注册数据
     * 
     * @param key key
     */
    void remove(String key);
    
    /**
     * Get current time from registry center. 获取注册中心当前时间
     * 
     * @param key key
     * @return current time from registry center
     */
    long getRegistryCenterTime(String key);
    
    /**
     * Get raw client for registry center client.
     ** 
     * @return registry center raw client
     */
    Object getRawClient();
}
