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

package org.apache.shardingsphere.elasticjob.lite.api.listener;

import lombok.Setter;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;
import org.apache.shardingsphere.elasticjob.infra.env.TimeService;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.infra.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.infra.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.lite.internal.guarantee.GuaranteeService;

import java.util.Set;

/**
 * Distributed once elasticjob listener. 分布式监听器，在分布式作业中【只执行一次】。若作业处理数据库数据，处理完成后只需一个节点完成数据清理任务即可。此类型任务处理复杂，需同步分布式环境下作业的状态同步，提供了超时设置来避免作业不同步导致的死锁，应谨慎使用。
 */
public abstract class AbstractDistributeOnceElasticJobListener implements ElasticJobListener {
    // 开始超时时间
    private final long startedTimeoutMilliseconds;
    // 开始等待对象
    private final Object startedWait = new Object();
    // 完成超时时间
    private final long completedTimeoutMilliseconds;
    // 完成等待对象
    private final Object completedWait = new Object();
    // 保证分布式任务全部开始和结束状态的服务
    @Setter
    private GuaranteeService guaranteeService;
    
    private final TimeService timeService = new TimeService();
    
    public AbstractDistributeOnceElasticJobListener(final long startedTimeoutMilliseconds, final long completedTimeoutMilliseconds) {
        this.startedTimeoutMilliseconds = startedTimeoutMilliseconds <= 0L ? Long.MAX_VALUE : startedTimeoutMilliseconds;
        this.completedTimeoutMilliseconds = completedTimeoutMilliseconds <= 0L ? Long.MAX_VALUE : completedTimeoutMilliseconds;
    }
    
    @Override
    public final void beforeJobExecuted(final ShardingContexts shardingContexts) {
        Set<Integer> shardingItems = shardingContexts.getShardingItemParameters().keySet(); // 所有分片项，不只是有本机的
        if (shardingItems.isEmpty()) {
            return;
        }
        guaranteeService.registerStart(shardingItems); // 注册作业分片项开始运行，/${JOB_NAME}/guarantee/started/${ITEM_INDEX}
        while (!guaranteeService.isRegisterStartSuccess(shardingItems)) { // /${JOB_NAME}/guarantee/started/${ITEM_INDEX} 所有分片节点是否都已被创建
            BlockUtils.waitingShortTime(); // 没有注册成功时则等待
        }
        if (guaranteeService.isAllStarted()) { // 判断是否所有的任务分片均启动完毕，当 /${JOB_NAME}/guarantee/started/ 目录下，所有作业分片项都开始运行，即运行总数等于作业分片总数( ShardingTotalCount )，代表所有的任务均启动完毕
            doBeforeJobExecutedAtLastStarted(shardingContexts); // 分布式环境中最后一个作业节点执行前的执行的方法
            guaranteeService.clearAllStartedInfo(); // 清理启动信息，删除 /${JOB_NAME}/guarantee/started/ 节点
            return;
        }
        long before = timeService.getCurrentMillis(); // 当前时间
        try {
            synchronized (startedWait) {
                startedWait.wait(startedTimeoutMilliseconds); // 不满足所有的分片项开始运行时，作业节点调用 Object#wait(...) 方法进行等待
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }
        if (timeService.getCurrentMillis() - before >= startedTimeoutMilliseconds) { // 等待超时
            guaranteeService.clearAllStartedInfo();  // 清理启动信息，删除 /${JOB_NAME}/guarantee/started/ 节点
            handleTimeout(startedTimeoutMilliseconds);
        }
    }
    
    @Override
    public final void afterJobExecuted(final ShardingContexts shardingContexts) {
        Set<Integer> shardingItems = shardingContexts.getShardingItemParameters().keySet();
        if (shardingItems.isEmpty()) {
            return;
        }
        guaranteeService.registerComplete(shardingItems);
        while (!guaranteeService.isRegisterCompleteSuccess(shardingItems)) {
            BlockUtils.waitingShortTime();
        }
        if (guaranteeService.isAllCompleted()) {
            doAfterJobExecutedAtLastCompleted(shardingContexts);
            guaranteeService.clearAllCompletedInfo();
            return;
        }
        long before = timeService.getCurrentMillis();
        try {
            synchronized (completedWait) {
                completedWait.wait(completedTimeoutMilliseconds);
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }
        if (timeService.getCurrentMillis() - before >= completedTimeoutMilliseconds) {
            guaranteeService.clearAllCompletedInfo();
            handleTimeout(completedTimeoutMilliseconds);
        }
    }
    
    private void handleTimeout(final long timeoutMilliseconds) {
        throw new JobSystemException("Job timeout. timeout mills is %s.", timeoutMilliseconds);
    }
    
    /**
     * Do before job executed at last sharding job started. 执行最后一个作业执行前的执行的方法，实现该抽象方法，完成自定义逻辑。
     *
     * @param shardingContexts sharding contexts
     */
    public abstract void doBeforeJobExecutedAtLastStarted(ShardingContexts shardingContexts);
    
    /**
     * Do after job executed at last sharding job completed. 分布式环境中最后一个作业执行后的执行的方法
     *
     * @param shardingContexts sharding contexts
     */
    public abstract void doAfterJobExecutedAtLastCompleted(ShardingContexts shardingContexts);
    
    /**
     * Notify waiting task start.
     */
    public void notifyWaitingTaskStart() {
        synchronized (startedWait) {
            startedWait.notifyAll();
        }
    }
    
    /**
     * Notify waiting task complete.
     */
    public void notifyWaitingTaskComplete() {
        synchronized (completedWait) {
            completedWait.notifyAll();
        }
    }
}
