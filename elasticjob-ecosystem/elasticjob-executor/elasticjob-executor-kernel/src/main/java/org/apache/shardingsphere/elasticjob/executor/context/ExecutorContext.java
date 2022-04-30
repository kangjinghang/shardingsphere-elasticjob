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

package org.apache.shardingsphere.elasticjob.executor.context;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.context.Reloadable;
import org.apache.shardingsphere.elasticjob.infra.context.ReloadablePostProcessor;
import org.apache.shardingsphere.elasticjob.infra.spi.ElasticJobServiceLoader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Executor context. 主要包括两个对象， {@link org.apache.shardingsphere.elasticjob.error.handler.JobErrorHandler} 和 {@link java.util.concurrent.ExecutorService}
 *
 * @see org.apache.shardingsphere.elasticjob.error.handler.JobErrorHandlerReloadable
 * @see org.apache.shardingsphere.elasticjob.infra.concurrent.ExecutorServiceReloadable
 */
public final class ExecutorContext {
    
    static {
        ElasticJobServiceLoader.registerTypedService(Reloadable.class);
    }
    // Key：reloadable.getType() （java.util.concurrent.ExecutorService / org.apache.shardingsphere.elasticjob.error.handler.JobErrorHandler），Value：实现类对象
    private final Map<String, Reloadable<?>> reloadableItems = new LinkedHashMap<>();
    
    public ExecutorContext(final JobConfiguration jobConfig) {
        ServiceLoader.load(Reloadable.class).forEach(each -> { // SPI 读取所有 Reloadable 实现类（即 ExecutorServiceReloadable 和 JobErrorHandlerReloadable 两个接口的实现类）
            ElasticJobServiceLoader.newTypedServiceInstance(Reloadable.class, each.getType(), new Properties()) // 依次实例化所有实现 Reloadable 接口的 SPI 对象
                    .ifPresent(reloadable -> reloadableItems.put(reloadable.getType(), reloadable)); // 保存到 reloadableItems
        });
        initReloadable(jobConfig);
    }
    // 调用 ReloadablePostProcessor 接口的 init 方法，初始化 Reloadable 对象
    private void initReloadable(final JobConfiguration jobConfig) {
        reloadableItems.values().stream().filter(each -> each instanceof ReloadablePostProcessor).forEach(each -> ((ReloadablePostProcessor) each).init(jobConfig));
    }
    
    /**
     * Reload all reloadable item if necessary.
     *
     * @param jobConfiguration job configuration
     */
    public void reloadIfNecessary(final JobConfiguration jobConfiguration) {
        reloadableItems.values().forEach(each -> each.reloadIfNecessary(jobConfiguration));
    }
    
    /**
     * Get instance.
     *
     * @param targetClass target class
     * @param <T>         target type
     * @return instance
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final Class<T> targetClass) {
        return (T) reloadableItems.get(targetClass.getName()).getInstance();
    }
    
    /**
     * Shutdown all closeable instances.
     */
    public void shutdown() {
        for (Reloadable<?> each : reloadableItems.values()) {
            try {
                each.close();
            } catch (final IOException ignored) {
            }
        }
    }
}
