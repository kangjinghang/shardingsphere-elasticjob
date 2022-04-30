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

package org.apache.shardingsphere.elasticjob.reg.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.exception.RegException;
import org.apache.shardingsphere.elasticjob.reg.exception.RegExceptionHandler;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener.State;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Registry center of ZooKeeper. 基于 Zookeeper 注册中心，使用 Apache Curator 进行 Zookeeper 注册中心。
 */
@Slf4j
public final class ZookeeperRegistryCenter implements CoordinatorRegistryCenter {
    
    @Getter(AccessLevel.PROTECTED)
    private final ZookeeperConfiguration zkConfig;
    // 缓存，key：作业名。通过 CuratorCache 实现监控整个树( Zookeeper目录 )的数据订阅和缓存，包括节点的状态，子节点的状态。
    private final Map<String, CuratorCache> caches = new ConcurrentHashMap<>();
    
    @Getter
    private CuratorFramework client;
    
    public ZookeeperRegistryCenter(final ZookeeperConfiguration zkConfig) {
        this.zkConfig = zkConfig;
    }
    
    @Override
    public void init() {
        log.debug("Elastic job: zookeeper registry center init, server lists is: {}.", zkConfig.getServerLists());
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getServerLists()) //  Zookeeper 失去链接后重新连接的一种策略：动态计算每次计算重连的间隔，时间间隔 = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))
                .retryPolicy(new ExponentialBackoffRetry(zkConfig.getBaseSleepTimeMilliseconds(), zkConfig.getMaxRetries(), zkConfig.getMaxSleepTimeMilliseconds()))
                .namespace(zkConfig.getNamespace());  // 命名空间。相同的作业集群使用相同的 Zookeeper 命名空间( ZookeeperConfiguration.namespace )
        if (0 != zkConfig.getSessionTimeoutMilliseconds()) {
            builder.sessionTimeoutMs(zkConfig.getSessionTimeoutMilliseconds());  // 会话超时时间，默认 60 * 1000 毫秒
        }
        if (0 != zkConfig.getConnectionTimeoutMilliseconds()) {
            builder.connectionTimeoutMs(zkConfig.getConnectionTimeoutMilliseconds()); // 连接超时时间，默认 15 * 1000 毫秒
        }
        if (!Strings.isNullOrEmpty(zkConfig.getDigest())) { // 认证
            builder.authorization("digest", zkConfig.getDigest().getBytes(StandardCharsets.UTF_8))
                    .aclProvider(new ACLProvider() {
                    
                        @Override
                        public List<ACL> getDefaultAcl() {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    
                        @Override
                        public List<ACL> getAclForPath(final String path) {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    });
        }
        client = builder.build();
        client.start();
        try { // 连接 Zookeeper
            if (!client.blockUntilConnected(zkConfig.getMaxSleepTimeMilliseconds() * zkConfig.getMaxRetries(), TimeUnit.MILLISECONDS)) {
                client.close();
                throw new KeeperException.OperationTimeoutException();
            }
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 关闭
    @Override
    public void close() {
        for (Entry<String, CuratorCache> each : caches.entrySet()) {
            each.getValue().close();
        }
        waitForCacheClose();
        CloseableUtils.closeQuietly(client);
    }
    
    /*
     *  // TODO 因为异步处理, 可能会导致client先关闭而cache还未关闭结束。等待Curator新版本解决这个bug。
     * sleep 500ms, let cache client close first and then client, otherwise will throw exception
     * reference：https://issues.apache.org/jira/browse/CURATOR-157
     */
    private void waitForCacheClose() {
        try {
            Thread.sleep(500L); // 等待500 ms, cache 先关闭再关闭 client, 否则会抛异常
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    // 获得数据，先从 CuratorCache 缓存获取，后从 Zookeeper 获取
    @Override
    public String get(final String key) {
        CuratorCache cache = findCuratorCache(key);  // 获取缓存
        if (null == cache) {
            return getDirectly(key); // 直接从 Zookeeper 获取
        }
        Optional<ChildData> resultInCache = cache.get(key); // 缓存中获取 value
        return resultInCache.map(v -> null == v.getData() ? null : new String(v.getData(), StandardCharsets.UTF_8)).orElseGet(() -> getDirectly(key));
    }
    
    private CuratorCache findCuratorCache(final String key) {
        for (Entry<String, CuratorCache> entry : caches.entrySet()) {
            if (key.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }
    // 直接从 Zookeeper 获取
    @Override
    public String getDirectly(final String key) {
        try {
            return new String(client.getData().forPath(key), StandardCharsets.UTF_8);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex); //  获得注册数据时，可能节点不存在，抛出 NodeExistsException，这种异常可以无视
            return null;
        }
    }
    // 获取子节点名称集合(降序) 与 OdevitySortByNameJobShardingStrategy#sharding(...) 结合看
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            List<String> result = client.getChildren().forPath(key);
            result.sort(Comparator.reverseOrder());
            return result;
         //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }
    // 获取子节点数量
    @Override
    public int getNumChildren(final String key) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if (null != stat) {
                return stat.getNumChildren();
            }
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return 0;
    }
    // 判断节点是否存在
    @Override
    public boolean isExisted(final String key) {
        try {
            return null != client.checkExists().forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return false;
        }
    }
    // 存储持久节点数据。逻辑等价于 insertOrUpdate 操作
    @Override
    public void persist(final String key, final String value) {
        try {
            if (!isExisted(key)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(key, value.getBytes(StandardCharsets.UTF_8));
            } else {
                update(key, value);
            }
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 使用事务校验键( key )存在才进行更新
    @Override
    public void update(final String key, final String value) {
        try {
            TransactionOp transactionOp = client.transactionOp();
            client.transaction().forOperations(transactionOp.check().forPath(key), transactionOp.setData().forPath(key, value.getBytes(StandardCharsets.UTF_8)));
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 存储临时节点数据。节点类型无法变更，因此如果数据已存在，需要先进行删除
    @Override
    public void persistEphemeral(final String key, final String value) {
        try {
            if (isExisted(key)) {
                client.delete().deletingChildrenIfNeeded().forPath(key); // 先进行删除
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(StandardCharsets.UTF_8));
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 存储顺序注册数据
    @Override
    public String persistSequential(final String key, final String value) {
        try {
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(key, value.getBytes(StandardCharsets.UTF_8));
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return null;
    }
    
    @Override
    public void persistEphemeralSequential(final String key) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 移除注册数据
    @Override
    public void remove(final String key) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    // 获取注册中心当前时间
    @Override
    public long getRegistryCenterTime(final String key) {
        long result = 0L;
        try {
            persist(key, "");
            result = client.checkExists().forPath(key).getMtime(); // 通过更新节点，获得该节点的最后更新时间( mtime )获得 Zookeeper 的时间
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        Preconditions.checkState(0L != result, "Cannot get registry center time.");
        return result;
    }
    
    @Override
    public Object getRawClient() {
        return client;
    }
    
    @Override
    public void addConnectionStateChangedEventListener(final ConnectionStateChangedEventListener listener) {
        CoordinatorRegistryCenter coordinatorRegistryCenter = this;
        client.getConnectionStateListenable().addListener((client, newState) -> {
            State state;
            switch (newState) {
                case CONNECTED:
                    state = State.CONNECTED;
                    break;
                case LOST:
                case SUSPENDED:
                    state = State.UNAVAILABLE;
                    break;
                case RECONNECTED:
                    state = State.RECONNECTED;
                    break;
                case READ_ONLY:
                default:
                    throw new IllegalStateException("Illegal registry center connection state: " + newState);
            }
            listener.onStateChanged(coordinatorRegistryCenter, state);
        });
    }
    
    @Override
    public void executeInTransaction(final List<TransactionOperation> transactionOperations) throws Exception {
        client.transaction().forOperations(toCuratorOps(transactionOperations));
    }

    private List<CuratorOp> toCuratorOps(final List<TransactionOperation> transactionOperations) {
        List<CuratorOp> result = new ArrayList<>(transactionOperations.size());
        TransactionOp transactionOp = client.transactionOp();
        for (TransactionOperation each : transactionOperations) {
            result.add(toCuratorOp(each, transactionOp)); // 将 TransactionOperation 转为 TransactionOp
        }
        return result;
    }
    
    private CuratorOp toCuratorOp(final TransactionOperation each, final TransactionOp transactionOp) {
        try {
            switch (each.getType()) {
                case CHECK_EXISTS:
                    return transactionOp.check().forPath(each.getKey());
                case ADD:
                    return transactionOp.create().forPath(each.getKey(), each.getValue().getBytes(StandardCharsets.UTF_8));
                case UPDATE:
                    return transactionOp.setData().forPath(each.getKey(), each.getValue().getBytes(StandardCharsets.UTF_8));
                case DELETE:
                    return transactionOp.delete().forPath(each.getKey());
                default:
                    throw new UnsupportedOperationException(each.toString());
            }
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            throw new RegException(ex);
        }
    }
    // 添加注册中心缓存，作业初始化注册时，初始化缓存
    @Override
    public void addCacheData(final String cachePath) {
        CuratorCache cache = CuratorCache.build(client, cachePath);
        try {
            cache.start();
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        caches.put(cachePath + "/", cache);
    }
    // 关闭作业缓存
    @Override
    public void evictCacheData(final String cachePath) {
        CuratorCache cache = caches.remove(cachePath + "/");
        if (null != cache) {
            cache.close();
        }
    }
    
    @Override
    public Object getRawCache(final String cachePath) {
        return caches.get(cachePath + "/");
    }
    // 在主节点执行操作
    @Override
    public void executeInLeader(final String key, final LeaderExecutionCallback callback) {
        try (LeaderLatch latch = new LeaderLatch(client, key)) { // 使用一个 Zookeeper 节点路径创建一个 LeaderLatch
            latch.start();
            latch.await(); // #start() 后，调用 #await() 等待拿到这把锁
            callback.execute(); // 如果有多个线程执行了相同节点路径的 LeaderLatch 的 #await() 后，同一时刻有且仅有一个线程可以继续执行，其他线程需要等待。当该线程释放( LeaderLatch#close() )后，下一个线程可以拿到该锁继续执行
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            handleException(ex);
        }
    }
    
    @Override
    public void watch(final String key, final DataChangedEventListener listener) {
        CuratorCache cache = caches.get(key + "/");
        cache.listenable().addListener((curatorType, oldData, newData) -> {
            if (null == newData && null == oldData) {
                return;
            }
            Type type = getTypeFromCuratorType(curatorType);
            String path = Type.DELETED == type ? oldData.getPath() : newData.getPath();
            if (path.isEmpty() || Type.IGNORED == type) { // 忽略掉非数据变化的事件，例如 event.type 为 CONNECTION_SUSPENDED、CONNECTION_RECONNECTED、CONNECTION_LOST、INITIALIZED 事件
                return;
            }
            byte[] data = Type.DELETED == type ? oldData.getData() : newData.getData();
            listener.onChange(new DataChangedEvent(type, path, null == data ? "" : new String(data, StandardCharsets.UTF_8)));
        });
    }
    
    private Type getTypeFromCuratorType(final CuratorCacheListener.Type curatorType) {
        switch (curatorType) {
            case NODE_CREATED:
                return Type.ADDED;
            case NODE_DELETED:
                return Type.DELETED;
            case NODE_CHANGED:
                return Type.UPDATED;
            default:
                return Type.IGNORED;
        }
    }
    
    private void handleException(final Exception ex) {
        if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new RegException(ex);
        }
    }
}
