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

package org.apache.shardingsphere.elasticjob.lite.internal.snapshot;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.shardingsphere.elasticjob.lite.internal.util.SensitiveInfoUtils;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperRegistryCenter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Snapshot service. 作业快照服务。使用Elastic-Job-Lite过程中可能会碰到一些分布式问题，导致作业运行不稳定。由于无法在生产环境调试，通过dump命令可以把作业内部相关信息dump出来，方便开发者debug分析； 另外为了不泄露隐私，已将相关信息中的ip地址以ip1, ip2…的形式过滤，可以在互联网上公开传输环境信息，便于进一步完善Elastic-Job。
 */
@Slf4j
public final class SnapshotService { // 使用案例：echo "dump" | nc 127.0.0.1 10024
    
    public static final String DUMP_COMMAND = "dump@";

    private final int port;
    
    private final CoordinatorRegistryCenter regCenter;
    
    private ServerSocket serverSocket;
    
    private volatile boolean closed;
    // 在作业配置的监控服务端口属性( JobConfiguration.monitorPort )启动 ServerSocket。一个作业对应一个作业监控端口，所以配置时，请不要重复端口噢。
    public SnapshotService(final CoordinatorRegistryCenter regCenter, final int port) {
        Preconditions.checkArgument(port >= 0 && port <= 0xFFFF, "Port value out of range: " + port);
        this.regCenter = regCenter;
        this.port = port;
    }
    
    /**
     * Start to listen. spring bean 初始化方法
     */
    public void listen() {
        try {
            log.info("ElasticJob: Snapshot service is running on port '{}'", openSocket(port));
        } catch (final IOException ex) {
            log.error("ElasticJob: Snapshot service listen failure, error is: ", ex);
        }
    }
    
    private int openSocket(final int port) throws IOException {
        serverSocket = new ServerSocket(port);
        int localPort = serverSocket.getLocalPort();
        String threadName = String.format("elasticjob-snapshot-service-%d", localPort);
        new Thread(() -> {
            while (!closed) {
                try {
                    process(serverSocket.accept()); // 等待 socket 连接
                } catch (final IOException ex) {
                    if (isIgnoredException()) {
                        return;
                    }
                    log.error("ElasticJob: Snapshot service open socket failure, error is: ", ex);
                }
            }
        }, threadName).start();
        return localPort;
    }
    
    private boolean isIgnoredException() {
        return serverSocket.isClosed();
    }
    
    private void process(final Socket socket) throws IOException {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                Socket ignored = socket) {
            String cmdLine = reader.readLine();
            if (null != cmdLine && cmdLine.startsWith(DUMP_COMMAND) && cmdLine.split("@").length == 2) { // 目前只支持 DUMP 命令
                List<String> result = new ArrayList<>();
                String jobName = cmdLine.split("@")[1]; // job name
                dumpDirectly("/" + jobName, jobName, result);
                outputMessage(writer, String.join("\n", SensitiveInfoUtils.filterSensitiveIps(result)) + "\n");
            }
        }
    }
    // 使用案例：echo "dump" | nc 127.0.0.1 10024
    private void dumpDirectly(final String path, final String jobName, final List<String> result) {
        for (String each : regCenter.getChildrenKeys(path)) {
            String zkPath = path + "/" + each;
            String zkValue = Optional.ofNullable(regCenter.get(zkPath)).orElse("");
            String cachePath = zkPath;
            String cacheValue = zkValue;
            // TODO Decoupling ZooKeeper
            if (regCenter instanceof ZookeeperRegistryCenter) {
                CuratorCache cache = (CuratorCache) regCenter.getRawCache("/" + jobName);
                if (null != cache) {
                    Optional<ChildData> cacheData = cache.get(zkPath);
                    cachePath = cacheData.map(ChildData::getPath).orElse("");
                    cacheValue = cacheData.map(ChildData::getData).map(String::new).orElse("");
                }
            }
            if (zkValue.equals(cacheValue) && zkPath.equals(cachePath)) {  // 判断 CuratorCache 缓存 和 注册中心 数据一致
                result.add(String.join(" | ", zkPath, zkValue)); // 当作业本地 CuratorCache 缓存 和注册中心数据相同时，只需 DUMP 出 [zkPath, zkValue]，方便看出本地和注册中心是否存在数据差异。
            } else {
                result.add(String.join(" | ", zkPath, zkValue, cachePath, cacheValue)); // 当作业本地 CuratorCache 缓存 和注册中心数据不一致时，DUMP 出 [zkPath, zkValue, treeCachePath, treeCacheValue]。
            }
            dumpDirectly(zkPath, jobName, result); // 递归
        }
    }
    
    private void outputMessage(final BufferedWriter outputWriter, final String msg) throws IOException {
        outputWriter.append(msg);
        outputWriter.flush();
    }
    
    /**
     * Close listener. spring bean 销毁方法
     */
    public void close() {
        closed = true;
        if (null != serverSocket && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (final IOException ex) {
                log.error("ElasticJob: Snapshot service close failure, error is: ", ex);
            }
        }
    }
}
