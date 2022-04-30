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

package org.apache.shardingsphere.elasticjob.lite.internal.schedule;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.error.handler.JobErrorHandlerPropertiesValidator;
import org.apache.shardingsphere.elasticjob.executor.ElasticJobExecutor;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.infra.listener.ElasticJobListenerFactory;
import org.apache.shardingsphere.elasticjob.infra.spi.ElasticJobServiceLoader;
import org.apache.shardingsphere.elasticjob.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.guarantee.GuaranteeService;
import org.apache.shardingsphere.elasticjob.lite.internal.setup.JobClassNameProviderFactory;
import org.apache.shardingsphere.elasticjob.lite.internal.setup.SetUpFacade;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Job scheduler. elastic-job 的作业调度器，作业执行环境初始化，构建和初始化 quartz 的 scheduler 和 jobDetail
 */
public final class JobScheduler {
    
    static {
        ElasticJobServiceLoader.registerTypedService(JobErrorHandlerPropertiesValidator.class);
    }
    
    private static final String JOB_EXECUTOR_DATA_MAP_KEY = "jobExecutor";
    // 注册中心
    @Getter
    private final CoordinatorRegistryCenter regCenter;
    // 作业配置
    @Getter
    private final JobConfiguration jobConfig;
    
    private final SetUpFacade setUpFacade;
    // 调度器门面对象
    private final SchedulerFacade schedulerFacade;
    // 作业门面对象
    private final LiteJobFacade jobFacade;
    
    private final ElasticJobExecutor jobExecutor;
    
    @Getter
    private final JobScheduleController jobScheduleController;
    
    public JobScheduler(final CoordinatorRegistryCenter regCenter, final ElasticJob elasticJob, final JobConfiguration jobConfig) {
        Preconditions.checkArgument(null != elasticJob, "Elastic job cannot be null.");
        this.regCenter = regCenter;
        Collection<ElasticJobListener> jobListeners = getElasticJobListeners(jobConfig); // 设置 作业监听器
        setUpFacade = new SetUpFacade(regCenter, jobConfig.getJobName(), jobListeners);
        String jobClassName = JobClassNameProviderFactory.getProvider().getJobClassName(elasticJob);
        this.jobConfig = setUpFacade.setUpJobConfiguration(jobClassName, jobConfig);
        schedulerFacade = new SchedulerFacade(regCenter, jobConfig.getJobName()); // 设置 调度器门面对象
        jobFacade = new LiteJobFacade(regCenter, jobConfig.getJobName(), jobListeners, findTracingConfiguration().orElse(null)); // 设置 作业门面对象
        validateJobProperties();
        jobExecutor = new ElasticJobExecutor(elasticJob, this.jobConfig, jobFacade); // 创建作业执行器
        setGuaranteeServiceForElasticJobListeners(regCenter, jobListeners);
        jobScheduleController = createJobScheduleController();
    }
    
    public JobScheduler(final CoordinatorRegistryCenter regCenter, final String elasticJobType, final JobConfiguration jobConfig) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(elasticJobType), "Elastic job type cannot be null or empty.");
        this.regCenter = regCenter;
        Collection<ElasticJobListener> jobListeners = getElasticJobListeners(jobConfig);
        setUpFacade = new SetUpFacade(regCenter, jobConfig.getJobName(), jobListeners);
        this.jobConfig = setUpFacade.setUpJobConfiguration(elasticJobType, jobConfig);
        schedulerFacade = new SchedulerFacade(regCenter, jobConfig.getJobName());
        jobFacade = new LiteJobFacade(regCenter, jobConfig.getJobName(), jobListeners, findTracingConfiguration().orElse(null));
        validateJobProperties();
        jobExecutor = new ElasticJobExecutor(elasticJobType, this.jobConfig, jobFacade);
        setGuaranteeServiceForElasticJobListeners(regCenter, jobListeners);
        jobScheduleController = createJobScheduleController();
    }
    
    private Collection<ElasticJobListener> getElasticJobListeners(final JobConfiguration jobConfig) {
        return jobConfig.getJobListenerTypes().stream()
                .map(type -> ElasticJobListenerFactory.createListener(type).orElseThrow(() -> new IllegalArgumentException(String.format("Can not find job listener type '%s'.", type))))
                .collect(Collectors.toList());
    }
    
    private Optional<TracingConfiguration<?>> findTracingConfiguration() {
        return jobConfig.getExtraConfigurations().stream().filter(each -> each instanceof TracingConfiguration).findFirst().map(extraConfig -> (TracingConfiguration<?>) extraConfig);
    }
    
    private void validateJobProperties() {
        validateJobErrorHandlerProperties();
    }
    
    private void validateJobErrorHandlerProperties() {
        if (null != jobConfig.getJobErrorHandlerType()) {
            ElasticJobServiceLoader.newTypedServiceInstance(JobErrorHandlerPropertiesValidator.class, jobConfig.getJobErrorHandlerType(), jobConfig.getProps())
                    .ifPresent(validator -> validator.validate(jobConfig.getProps()));
        }
    }
    
    private void setGuaranteeServiceForElasticJobListeners(final CoordinatorRegistryCenter regCenter, final Collection<ElasticJobListener> elasticJobListeners) {
        GuaranteeService guaranteeService = new GuaranteeService(regCenter, jobConfig.getJobName());
        for (ElasticJobListener each : elasticJobListeners) {
            if (each instanceof AbstractDistributeOnceElasticJobListener) {
                ((AbstractDistributeOnceElasticJobListener) each).setGuaranteeService(guaranteeService);
            }
        }
    }
    // 创建 quartz 调度器的封装 JobScheduleController，并且初始化作业，作业开始调度。
    private JobScheduleController createJobScheduleController() {
        JobScheduleController result = new JobScheduleController(createScheduler(), createJobDetail(), getJobConfig().getJobName());
        JobRegistry.getInstance().registerJob(getJobConfig().getJobName(), result); // 添加 作业调度控制器实例
        registerStartUpInfo(); // 注册 作业启动信息
        return result;
    }
    // 创建 quartz 的 Scheduler
    private Scheduler createScheduler() {
        Scheduler result;
        try {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getQuartzProps());
            result = factory.getScheduler();
            result.getListenerManager().addTriggerListener(schedulerFacade.newJobTriggerListener()); // 添加 triggerListener，使用 TriggerListener 监听被错过执行的作业分片项
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }
    
    private Properties getQuartzProps() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1"); // Quartz 线程数：1，一个作业( ElasticJob )的调度，需要配置独有的一个作业调度器( JobScheduler )，两者是 1 : 1 的关系。
        result.put("org.quartz.scheduler.instanceName", getJobConfig().getJobName());
        result.put("org.quartz.jobStore.misfireThreshold", "1"); // 设置最大允许超过 1 毫秒，作业分片项即被视为错过执行
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName()); // 作业关闭钩子
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString()); // 关闭时，清理所有资源
        return result;
    }
    // 创建 quartz 的 JobDetail
    private JobDetail createJobDetail() {
        JobDetail result = JobBuilder.newJob(LiteJob.class).withIdentity(getJobConfig().getJobName()).build(); // 参数是 LiteJob，因此，每次 Quartz 到达调度时间时，会创建该对象进行作业执行。
        result.getJobDataMap().put(JOB_EXECUTOR_DATA_MAP_KEY, jobExecutor); // 将 ElasticJobExecutor 注入
        return result;
    }
    // 注册作业启动信息
    private void registerStartUpInfo() {
        JobRegistry.getInstance().registerRegistryCenter(jobConfig.getJobName(), regCenter); // 添加 注册中心实例
        JobRegistry.getInstance().addJobInstance(jobConfig.getJobName(), new JobInstance()); // 添加 作业运行实例
        JobRegistry.getInstance().setCurrentShardingTotalCount(jobConfig.getJobName(), jobConfig.getShardingTotalCount());
        setUpFacade.registerStartUpInfo(!jobConfig.isDisabled());
    }
    
    /**
     * Shutdown job.
     */
    public void shutdown() {
        setUpFacade.tearDown();
        schedulerFacade.shutdownInstance();
        jobExecutor.shutdown();
    }
}
