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

package org.apache.shardingsphere.elasticjob.api;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

/**
 * ElasticJob configuration. 配置类
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class JobConfiguration {
    // 作业名称。必填
    private final String jobName;
    // cron表达式，用于控制作业触发时间。选填
    private final String cron;
    
    private final String timeZone;
    // 作业分片总数。如果一个作业启动超过作业分片总数的节点，只有 shardingTotalCount 会执行作业。必填
    private final int shardingTotalCount;
    // 分片序列号和参数。选填。分片序列号和参数用等号分隔，多个键值对用逗号分隔，分片序列号从0开始，不可大于或等于作业分片总数，如：0=a,1=b,2=c
    private final String shardingItemParameters;
    // 作业自定义参数。选填，可通过传递该参数为作业调度的业务方法传参，用于实现带参数的作业。例：每次获取的数据量、作业实例从数据库读取的主键等
    private final String jobParameter;
    // 监控作业运行时状态。默认为 false。选填
    private final boolean monitorExecution;
    // 是否开启作业执行失效转移。开启表示如果作业在一次作业执行中途宕机，允许将该次未完成的作业在另一作业节点上补偿执行。默认为 false。选填
    private final boolean failover;
    // 是否开启错过作业重新执行。默认为 true。选填
    private final boolean misfire;
    // 设置最大容忍的本机与注册中心的时间误差秒数。默认为 -1，不检查时间误差。选填
    private final int maxTimeDiffSeconds;
    // 修复作业服务器不一致状态服务调度间隔时间，配置为小于1的任意值表示不执行修复。默认为 10
    private final int reconcileIntervalMinutes;
    // 作业分片策略实现类全路径。默认为 AVG_ALLOCATION
    private final String jobShardingStrategyType;
    
    private final String jobExecutorServiceHandlerType;
    
    private final String jobErrorHandlerType;
    
    private final Collection<String> jobListenerTypes;
    
    private final Collection<JobExtraConfiguration> extraConfigurations;
    // 作业描述。选填
    private final String description;
    // 作业属性配置。选填
    private final Properties props;
    // 作业是否禁用执行。默认为 false。选填
    private final boolean disabled;
    // 设置使用本地作业配置覆盖注册中心的作业配置。默认为 false。选填。建议使用运维平台配置作业配置，统一管理。
    private final boolean overwrite;
    
    private final String label;
    
    private final boolean staticSharding;
    
    /**
     * Create ElasticJob configuration builder.
     *
     * @param jobName job name
     * @param shardingTotalCount sharding total count
     * @return ElasticJob configuration builder
     */
    public static Builder newBuilder(final String jobName, final int shardingTotalCount) {
        return new Builder(jobName, shardingTotalCount);
    }
    // 使用该类配置 LiteJobConfiguration 属性，调用 #build() 方法最终生成作业配置
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        
        private final String jobName;
        
        private String cron;
        
        private String timeZone;
        
        private final int shardingTotalCount;
        
        private String shardingItemParameters = "";
        
        private String jobParameter = "";
        
        private boolean monitorExecution = true;
        
        private boolean failover;
        
        private boolean misfire = true;
        
        private int maxTimeDiffSeconds = -1;
        
        private int reconcileIntervalMinutes = 10;
        
        private String jobShardingStrategyType;
        
        private String jobExecutorServiceHandlerType;
        
        private String jobErrorHandlerType;
    
        private final Collection<String> jobListenerTypes = new ArrayList<>();

        private final Collection<JobExtraConfiguration> extraConfigurations = new LinkedList<>();
        
        private String description = "";
        
        private final Properties props = new Properties();
        
        private boolean disabled;
        
        private boolean overwrite;
    
        private String label;
        
        private boolean staticSharding;
    
        /**
         * Cron expression.
         *
         * @param cron cron expression
         * @return job configuration builder
         */
        public Builder cron(final String cron) {
            if (null != cron) {
                this.cron = cron;
            }
            return this;
        }
    
        /**
         * time zone.
         *
         * @param timeZone the time zone
         * @return job configuration builder
         */
        public Builder timeZone(final String timeZone) {
            if (null != timeZone) {
                this.timeZone = timeZone;
            }
            return this;
        }
        
        /**
         * Set mapper of sharding items and sharding parameters.
         *
         * <p>
         * sharding item and sharding parameter split by =, multiple sharding items and sharding parameters split by comma, just like map.
         * Sharding item start from zero, cannot equal to great than sharding total count.
         *
         * For example:
         * 0=a,1=b,2=c
         * </p>
         *
         * @param shardingItemParameters mapper of sharding items and sharding parameters
         * @return job configuration builder
         */
        public Builder shardingItemParameters(final String shardingItemParameters) {
            if (null != shardingItemParameters) {
                this.shardingItemParameters = shardingItemParameters;
            }
            return this;
        }
        
        /**
         * Set job parameter.
         *
         * @param jobParameter job parameter
         *
         * @return job configuration builder
         */
        public Builder jobParameter(final String jobParameter) {
            if (null != jobParameter) {
                this.jobParameter = jobParameter;
            }
            return this;
        }
        
        /**
         * Set enable or disable monitor execution.
         *
         * <p>
         * For short interval job, it is better to disable monitor execution to improve performance. 
         * It can't guarantee repeated data fetch and can't failover if disable monitor execution, please keep idempotence in job.
         *
         * For long interval job, it is better to enable monitor execution to guarantee fetch data exactly once.
         * </p>
         *
         * @param monitorExecution monitor job execution status 
         * @return ElasticJob configuration builder
         */
        public Builder monitorExecution(final boolean monitorExecution) {
            this.monitorExecution = monitorExecution;
            return this;
        }
        
        /**
         * Set enable failover.
         *
         * <p>
         * Only for `monitorExecution` enabled.
         * </p> 
         *
         * @param failover enable or disable failover
         * @return job configuration builder
         */
        public Builder failover(final boolean failover) {
            this.failover = failover;
            return this;
        }
        
        /**
         * Set enable misfire.
         *
         * @param misfire enable or disable misfire
         * @return job configuration builder
         */
        public Builder misfire(final boolean misfire) {
            this.misfire = misfire;
            return this;
        }
        
        /**
         * Set max tolerate time different seconds between job server and registry center.
         *
         * <p>
         * ElasticJob will throw exception if exceed max tolerate time different seconds.
         * -1 means do not check.
         * </p>
         *
         * @param maxTimeDiffSeconds max tolerate time different seconds between job server and registry center
         * @return ElasticJob configuration builder
         */
        public Builder maxTimeDiffSeconds(final int maxTimeDiffSeconds) {
            this.maxTimeDiffSeconds = maxTimeDiffSeconds;
            return this;
        }
        
        /**
         * Set reconcile interval minutes for job sharding status.
         *
         * <p>
         * Monitor the status of the job server at regular intervals, and resharding if incorrect.
         * </p>
         *
         * @param reconcileIntervalMinutes reconcile interval minutes for job sharding status
         * @return ElasticJob configuration builder
         */
        public Builder reconcileIntervalMinutes(final int reconcileIntervalMinutes) {
            this.reconcileIntervalMinutes = reconcileIntervalMinutes;
            return this;
        }
        
        /**
         * Set job sharding strategy type.
         *
         * <p>
         * Default for {@code AverageAllocationJobShardingStrategy}.
         * </p>
         *
         * @param jobShardingStrategyType job sharding strategy type
         * @return ElasticJob configuration builder
         */
        public Builder jobShardingStrategyType(final String jobShardingStrategyType) {
            if (null != jobShardingStrategyType) {
                this.jobShardingStrategyType = jobShardingStrategyType;
            }
            return this;
        }
        
        /**
         * Set job executor service handler type.
         *
         * @param jobExecutorServiceHandlerType job executor service handler type
         * @return job configuration builder
         */
        public Builder jobExecutorServiceHandlerType(final String jobExecutorServiceHandlerType) {
            this.jobExecutorServiceHandlerType = jobExecutorServiceHandlerType;
            return this;
        }
        
        /**
         * Set job error handler type.
         *
         * @param jobErrorHandlerType job error handler type
         * @return job configuration builder
         */
        public Builder jobErrorHandlerType(final String jobErrorHandlerType) {
            this.jobErrorHandlerType = jobErrorHandlerType;
            return this;
        }
        
        /**
         * Set job listener types.
         *
         * @param jobListenerTypes job listener types
         * @return ElasticJob configuration builder
         */
        public Builder jobListenerTypes(final String... jobListenerTypes) {
            this.jobListenerTypes.addAll(Arrays.asList(jobListenerTypes));
            return this;
        }
        
        /**
         * Add extra configurations.
         *
         * @param extraConfig job extra configuration
         * @return job configuration builder
         */
        public Builder addExtraConfigurations(final JobExtraConfiguration extraConfig) {
            extraConfigurations.add(extraConfig);
            return this;
        }
        
        /**
         * Set job description.
         *
         * @param description job description
         * @return job configuration builder
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }
        
        /**
         * Set property.
         *
         * @param key property key
         * @param value property value
         * @return job configuration builder
         */
        public Builder setProperty(final String key, final String value) {
            props.setProperty(key, value);
            return this;
        }
        
        /**
         * Set whether disable job when start.
         * 
         * <p>
         * Using in job deploy, start job together after deploy.
         * </p>
         *
         * @param disabled whether disable job when start
         * @return ElasticJob configuration builder
         */
        public Builder disabled(final boolean disabled) {
            this.disabled = disabled;
            return this;
        }
        
        /**
         * Set whether overwrite local configuration to registry center when job startup. 
         * 
         * <p>
         *  If overwrite enabled, every startup will use local configuration.
         * </p>
         *
         * @param overwrite whether overwrite local configuration to registry center when job startup
         * @return ElasticJob configuration builder
         */
        public Builder overwrite(final boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }
        
        /**
         * Set label.
         *
         * @param label label
         * @return ElasticJob configuration builder
         */
        public Builder label(final String label) {
            this.label = label;
            return this;
        }
        
        /**
         * Set static sharding.
         *
         * @param staticSharding static sharding
         * @return ElasticJob configuration builder
         */
        public Builder staticSharding(final boolean staticSharding) {
            this.staticSharding = staticSharding;
            return this;
        }
        
        /**
         * Build ElasticJob configuration.
         * 
         * @return ElasticJob configuration
         */
        public final JobConfiguration build() {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName), "jobName can not be empty.");
            Preconditions.checkArgument(shardingTotalCount > 0, "shardingTotalCount should larger than zero.");
            return new JobConfiguration(jobName, cron, timeZone, shardingTotalCount, shardingItemParameters, jobParameter,
                    monitorExecution, failover, misfire, maxTimeDiffSeconds, reconcileIntervalMinutes,
                    jobShardingStrategyType, jobExecutorServiceHandlerType, jobErrorHandlerType, jobListenerTypes,
                    extraConfigurations, description, props, disabled, overwrite, label, staticSharding);
        }
    }
}
