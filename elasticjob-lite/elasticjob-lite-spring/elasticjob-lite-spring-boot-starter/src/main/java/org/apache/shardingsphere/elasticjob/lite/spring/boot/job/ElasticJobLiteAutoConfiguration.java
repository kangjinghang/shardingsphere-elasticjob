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

package org.apache.shardingsphere.elasticjob.lite.spring.boot.job;

import org.apache.shardingsphere.elasticjob.lite.spring.boot.reg.ElasticJobRegistryCenterConfiguration;
import org.apache.shardingsphere.elasticjob.lite.spring.boot.reg.snapshot.ElasticJobSnapshotServiceConfiguration;
import org.apache.shardingsphere.elasticjob.lite.spring.boot.tracing.ElasticJobTracingConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * ElasticJob-Lite auto configuration.  自动配置主入口，import 其他的自动配置
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
@ConditionalOnProperty(name = "elasticjob.enabled", havingValue = "true", matchIfMissing = true)
@Import({ElasticJobRegistryCenterConfiguration.class, ElasticJobTracingConfiguration.class, ElasticJobSnapshotServiceConfiguration.class})
@EnableConfigurationProperties(ElasticJobProperties.class)
public class ElasticJobLiteAutoConfiguration {
    
    @Configuration(proxyBeanMethods = false)
    @Import({ElasticJobBootstrapConfiguration.class, ScheduleJobBootstrapStartupRunner.class})
    protected static class ElasticJobConfiguration {
    }
}
