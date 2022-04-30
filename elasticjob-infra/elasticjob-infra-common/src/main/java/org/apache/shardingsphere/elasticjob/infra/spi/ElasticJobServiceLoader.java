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

package org.apache.shardingsphere.elasticjob.infra.spi;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.elasticjob.infra.context.Reloadable;
import org.apache.shardingsphere.elasticjob.infra.handler.threadpool.JobExecutorServiceHandler;
import org.apache.shardingsphere.elasticjob.infra.spi.exception.ServiceLoaderInstantiationException;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ElasticJob service loader.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ElasticJobServiceLoader {
    // Key：继承 TypedSPI 的接口，如：JobExecutorServiceHandler，Value：实现 JobExecutorServiceHandler 接口的实例对象map（Key：TypedSPI 的 getType()返回值，Value：实例化好的对象）
    private static final ConcurrentMap<Class<? extends TypedSPI>, ConcurrentMap<String, TypedSPI>> TYPED_SERVICES = new ConcurrentHashMap<>();
    // Key：继承TypedSPI 的接口，如：Reloadable，Value：实现 Reloadable 接口的 Class map（Key：TypedSPI 的 getType()返回值，Value：实现类的 Class 对象）
    private static final ConcurrentMap<Class<? extends TypedSPI>, ConcurrentMap<String, Class<? extends TypedSPI>>> TYPED_SERVICE_CLASSES = new ConcurrentHashMap<>();

    /**
     * Register typeSPI service. 注册继承 TypedSPI 接口的 typedService 接口的所有实现类对象到 TYPED_SERVICES
     *
     * @param typedService typed service 继承 TypedSPI 接口的 typedService 接口，如 JobExecutorServiceHandler
     * @param <T> class of service
     */
    public static <T extends TypedSPI> void registerTypedService(final Class<T> typedService) {
        if (TYPED_SERVICES.containsKey(typedService)) {
            return;
        } // ServiceLoader.load(typedService).forEach() 遍历每一个 typedService 的 实现类实例化好的对象
        ServiceLoader.load(typedService).forEach(each -> registerTypedServiceClass(typedService, each)); // each 每一个具体实现类的 Class 对象
    }
    
    private static <T extends TypedSPI> void registerTypedServiceClass(final Class<T> typedService, final TypedSPI instance) {
        TYPED_SERVICES.computeIfAbsent(typedService, unused -> new ConcurrentHashMap<>()).putIfAbsent(instance.getType(), instance);
        TYPED_SERVICE_CLASSES.computeIfAbsent(typedService, unused -> new ConcurrentHashMap<>()).putIfAbsent(instance.getType(), instance.getClass());
    }
    
    /**
     * Get cached typed instance. 从缓存中获取实现了 typedServiceInterface 接口的 对象
     *
     * @param typedServiceInterface typed service interface
     * @param type type
     * @param <T> class of service
     * @return cached typed service instance
     */
    public static <T extends TypedSPI> Optional<T> getCachedTypedServiceInstance(final Class<T> typedServiceInterface, final String type) {
        return Optional.ofNullable(TYPED_SERVICES.get(typedServiceInterface)).map(services -> (T) services.get(type));
    }
    
    /**
     * New typed instance. 实例化实现 typedServiceInterface 接口的 SPI 对象
     *
     * @param typedServiceInterface typed service interface 要实例化的 typedServiceInterface 接口
     * @param type type
     * @param props properties
     * @param <T> class of service
     * @return new typed service instance 实例化好的 SPI 对象
     */
    public static <T extends TypedSPI> Optional<T> newTypedServiceInstance(final Class<T> typedServiceInterface, final String type, final Properties props) {
        Optional<T> result = Optional.ofNullable(TYPED_SERVICE_CLASSES.get(typedServiceInterface)).map(serviceClasses -> serviceClasses.get(type)).map(clazz -> (T) newServiceInstance(clazz));
        if (result.isPresent() && result.get() instanceof SPIPostProcessor) {
            ((SPIPostProcessor) result.get()).init(props);
        }
        return result;
    }
    
    private static Object newServiceInstance(final Class<?> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (final InstantiationException | NoSuchMethodException | IllegalAccessException ex) {
            throw new ServiceLoaderInstantiationException(clazz, ex);
        } catch (final InvocationTargetException ex) {
            throw new ServiceLoaderInstantiationException(clazz, ex.getCause());
        }
    }
}
