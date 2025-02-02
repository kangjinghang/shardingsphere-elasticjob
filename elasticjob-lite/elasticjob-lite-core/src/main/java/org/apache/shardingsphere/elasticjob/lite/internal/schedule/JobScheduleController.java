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
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import java.util.TimeZone;

/**
 * Job schedule controller. 作业调度控制器，quartz 调度器的封装
 */
@RequiredArgsConstructor
public final class JobScheduleController {
    // quartz scheduler
    private final Scheduler scheduler;
    // quartz jobDetail
    private final JobDetail jobDetail;
    // 触发器编号，目前使用工作名字( jobName )
    private final String triggerIdentity;
    
    /**
     * Schedule job.  调度作业
     * 
     * @param cron CRON expression
     * @param timeZone the time zone
     */
    public void scheduleJob(final String cron, final String timeZone) {
        try {
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, createCronTrigger(cron, timeZone));
            }
            scheduler.start();
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * Reschedule job.  重新调度作业
     * 
     * @param cron CRON expression
     * @param timeZone the time zone
     */
    public synchronized void rescheduleJob(final String cron, final String timeZone) {
        try {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger && !cron.equals(trigger.getCronExpression())) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createCronTrigger(cron, timeZone));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * Reschedule OneOff job.
     */
    public synchronized void rescheduleJob() {
        try {
            SimpleTrigger trigger = (SimpleTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createOneOffTrigger());
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    // 创建触发器，#withMisfireHandlingInstructionDoNothing() 设置 Quartz 系统不会立刻再执行任务，而是等到距离目前时间最近的预计时间执行。重新执行被错过执行的作业交给 Elastic-Job-Lite 处理
    private Trigger createCronTrigger(final String cron, final String timeZoneString) {
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(
                CronScheduleBuilder.cronSchedule(cron).inTimeZone(parseTimeZoneString(timeZoneString)).withMisfireHandlingInstructionDoNothing()
        ).build();
    }

    /**
     * Get the TimeZone for the time zone specification.
     *
     * @param timeZoneString must start with "GMT", such as "GMT+8:00"
     * @return the specified TimeZone, or the GMT zone if the `timeZoneString` cannot be understood.
     */
    private TimeZone parseTimeZoneString(final String timeZoneString) {
        if (Strings.isNullOrEmpty(timeZoneString)) {
            return TimeZone.getDefault();
        }
        Preconditions.checkArgument(timeZoneString.startsWith("GMT"), "Invalid time zone specification '%s'.", timeZoneString);
        return TimeZone.getTimeZone(timeZoneString);
    }
    
    /**
     * Judge job is pause or not. 判断作业是否暂停
     * 
     * @return job is pause or not
     */
    public synchronized boolean isPaused() {
        try {
            return !scheduler.isShutdown() && Trigger.TriggerState.PAUSED == scheduler.getTriggerState(new TriggerKey(triggerIdentity));
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * Pause job. 暂停本地作业调度
     */
    public synchronized void pauseJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.pauseAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * Resume job. 恢复本地作业调度
     */
    public synchronized void resumeJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.resumeAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * Trigger job. 立刻启动作业
     */
    public synchronized void triggerJob() {
        try {
            if (scheduler.isShutdown()) {
                return;
            }
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, createOneOffTrigger());
            } else {
                scheduler.triggerJob(jobDetail.getKey());
            }
            if (!scheduler.isStarted()) {
                scheduler.start();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    private Trigger createOneOffTrigger() {
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(SimpleScheduleBuilder.simpleSchedule()).build();
    }
    
    /**
     * Shutdown scheduler. 关闭调度器
     */
    public synchronized void shutdown() {
        shutdown(false);
    }
    
    /**
     * Shutdown scheduler graceful.
     * @param isCleanShutdown if wait jobs complete
     */
    public synchronized void shutdown(final boolean isCleanShutdown) {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown(isCleanShutdown);
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
}
