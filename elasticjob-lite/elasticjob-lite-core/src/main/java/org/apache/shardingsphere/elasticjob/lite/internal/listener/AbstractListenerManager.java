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

package org.apache.shardingsphere.elasticjob.lite.internal.listener;

import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

/**
 * Listener manager. 作业注册中心的监听器管理者的抽象类，其他服务监听器管理者需要继承此类，实现服务的监听管理接入的监听器实现
 */
public abstract class AbstractListenerManager {
    
    private final JobNodeStorage jobNodeStorage;
    
    protected AbstractListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
    }
    
    /**
     * Start listener. 开启监听器，将作业注册中心的监听器添加到注册中心 CuratorCache 的监听者里
     */
    public abstract void start(); // 子类实现此方法实现监听器初始化。目前所有子类的实现都是将自己管理的注册中心监听器调用 #addDataListener()
    // 添加注册中心监听器
    protected void addDataListener(final DataChangedEventListener listener) {
        jobNodeStorage.addDataListener(listener);
    }
}
