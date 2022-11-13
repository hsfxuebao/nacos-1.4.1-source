/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;
import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) {
        group = null2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listener.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group);
            }
        }
    }

    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        // todo 获取到指定文件对应的CacheData
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        // 将监听器添加到CacheData
        for (Listener listener : listeners) {
            // todo
            cache.addListener(listener);
        }
    }

    /**
     * Add listeners for tenant with content.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param content   content
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content,
            List<? extends Listener> listeners) throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        cache.setContent(content);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group, tenant);
            }
        }
    }

    private void removeCache(String dataId, String group) {
        String groupKey = GroupKey.getKey(dataId, group);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", this.agent.getName(), groupKey);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        String key = GroupKey.getKey(dataId, group);
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group);
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            lastCacheData = cacheData;
            lastCacheData.setTaskId(taskId);
        }
        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", this.agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
        return lastCacheData;
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        // 构建文件key，格式为：  配置文件名+groupId
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        // // 从缓存cacheMap中获取指定文件的cacheData, 若缓存中有该cacheData，则直接返回
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }

        // 创建CacheData
        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
        // multiple listeners on the same dataid+group and race condition
        // 将cacheData写入到cacheMap
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            //fix issue # 1317
            if (enableRemoteSyncConfig) {
                // todo 从远程config server获取最新的配置信息
                String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                // 将获取的最新数据写入到cacheData
                cacheData.setContent(ct[0]);
            }
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            cacheData.setTaskId(taskId);
            lastCacheData = cacheData;
        }

        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());

        return lastCacheData;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    public String[] getServerConfig(String dataId, String group, String tenant, long readTimeout)
            throws NacosException {
        String[] ct = new String[2];
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }

        HttpRestResult<String> result = null;
        try {
            Map<String, String> params = new HashMap<String, String>(3);
            if (StringUtils.isBlank(tenant)) {
                params.put("dataId", dataId);
                params.put("group", group);
            } else {
                params.put("dataId", dataId);
                params.put("group", group);
                params.put("tenant", tenant);
            }
            // 发送请求
            result = agent.httpGet(Constants.CONFIG_CONTROLLER_PATH, null, params, agent.getEncode(), readTimeout);
        } catch (Exception ex) {
            String message = String
                    .format("[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s",
                            agent.getName(), dataId, group, tenant);
            LOGGER.error(message, ex);
            throw new NacosException(NacosException.SERVER_ERROR, ex);
        }

        switch (result.getCode()) {
            case HttpURLConnection.HTTP_OK:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, result.getData());
                ct[0] = result.getData();
                if (result.getHeader().getValue(CONFIG_TYPE) != null) {
                    ct[1] = result.getHeader().getValue(CONFIG_TYPE);
                } else {
                    ct[1] = ConfigType.TEXT.getType();
                }
                return ct;
            case HttpURLConnection.HTTP_NOT_FOUND:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, null);
                return ct;
            case HttpURLConnection.HTTP_CONFLICT: {
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
            case HttpURLConnection.HTTP_FORBIDDEN: {
                LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}", agent.getName(),
                        dataId, group, tenant);
                throw new NacosException(result.getCode(), result.getMessage());
            }
            default: {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", agent.getName(),
                        dataId, group, tenant, result.getCode());
                throw new NacosException(result.getCode(),
                        "http error, code=" + result.getCode() + ",dataId=" + dataId + ",group=" + group + ",tenant="
                                + tenant);
            }
        }
    }

    private void checkLocalConfig(CacheData cacheData) {
        final String dataId = cacheData.dataId;
        final String group = cacheData.group;
        final String tenant = cacheData.tenant;
        // 获取到本地配置文件
        File path = LocalConfigInfoProcessor.getFailoverFile(agent.getName(), dataId, group, tenant);

        // 若配置的是不使用本地配置文件，但该配置文件在本地又存在，那就使用它
        if (!cacheData.isUseLocalConfigInfo() && path.exists()) {
            // 获取本地配置文件内容
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            // 计算出该内容的md5
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            // 设置 使用本地配置文件属性 为true
            cacheData.setUseLocalConfigInfo(true);
            // 将本地配置文件的最后修改时间写入到cacheData
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            // 将本地配置文件内容写入到cacheData
            cacheData.setContent(content);

            LOGGER.warn(
                    "[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            return;
        }

        // If use local config info, then it doesn't notify business listener and notify after getting from server.
        // 若配置的是使用本地配置文件，但这个文件又不存在
        if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
            cacheData.setUseLocalConfigInfo(false);
            LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", agent.getName(),
                    dataId, group, tenant);
            return;
        }

        // When it changed.
        // 若设置的是使用本地配置文件，且文件也存在，且这个本地配置文件发生了变化
        if (cacheData.isUseLocalConfigInfo() && path.exists() && cacheData.getLocalConfigInfoVersion() != path
                .lastModified()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);
            LOGGER.warn(
                    "[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
        }
    }

    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * Check config info.
     */
    public void checkConfigInfo() {
        // Dispatch taskes.
        // cacheMap的key为配置文件的key(配置文件名称+groupId)
        // value为CacheData（每个配置文件都会有一个CacheData，用于存放来自于Server的配置文件数据）
        int listenerSize = cacheMap.size();
        // Round up the longingTaskCount.
        // 向上取整
        int longingTaskCount = (int) Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize());
        if (longingTaskCount > currentLongingTaskCount) {
            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                // The task list is no order.So it maybe has issues when changing.
                // todo 执行长轮询任务
                executorService.execute(new LongPollingRunnable(i));
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }

    /**
     * Fetch the dataId list from server.
     *
     * @param cacheDatas              CacheDatas for config infomations.
     * @param inInitializingCacheList initial cache lists.
     * @return String include dataId and group (ps: it maybe null).
     * @throws Exception Exception.
     */
    List<String> checkUpdateDataIds(List<CacheData> cacheDatas, List<String> inInitializingCacheList) throws Exception {
        StringBuilder sb = new StringBuilder();
        // 将cacheDatas中的所有配置文件名称全部拼接为String，发送给Server，
        // 让Server去检测这些文件是否发生了变更
        for (CacheData cacheData : cacheDatas) {
            if (!cacheData.isUseLocalConfigInfo()) {
                sb.append(cacheData.dataId).append(WORD_SEPARATOR);
                sb.append(cacheData.group).append(WORD_SEPARATOR);
                if (StringUtils.isBlank(cacheData.tenant)) {
                    sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
                } else {
                    sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
                    sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
                }
                if (cacheData.isInitializing()) {
                    // It updates when cacheData occours in cacheMap by first time.
                    inInitializingCacheList
                            .add(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant));
                }
            }
        }
        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        // todo 将这个配置文件名称String发送给Server
        return checkUpdateConfigStr(sb.toString(), isInitializingCacheList);
    }

    /**
     * Fetch the updated dataId list from server.
     *
     * @param probeUpdateString       updated attribute string value.
     * @param isInitializingCacheList initial cache lists.
     * @return The updated dataId list(ps: it maybe null).
     * @throws IOException Exception.
     */
    List<String> checkUpdateConfigStr(String probeUpdateString, boolean isInitializingCacheList) throws Exception {

        Map<String, String> params = new HashMap<String, String>(2);
        params.put(Constants.PROBE_MODIFY_REQUEST, probeUpdateString);
        Map<String, String> headers = new HashMap<String, String>(2);
        headers.put("Long-Pulling-Timeout", "" + timeout);

        // told server do not hang me up if new initializing cacheData added in
        if (isInitializingCacheList) {
            headers.put("Long-Pulling-Timeout-No-Hangup", "true");
        }

        if (StringUtils.isBlank(probeUpdateString)) {
            return Collections.emptyList();
        }

        try {
            // In order to prevent the server from handling the delay of the client's long task,
            // increase the client's read timeout to avoid this problem.

            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1);
            // 提交请求
            HttpRestResult<String> result = agent
                    .httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener", headers, params, agent.getEncode(),
                            readTimeoutMs);

            if (result.ok()) {
                setHealthServer(true);
                // todo 解析result，返回所有发生变更的配置文件的key
                return parseUpdateDataIdResponse(result.getData());
            } else {
                setHealthServer(false);
                LOGGER.error("[{}] [check-update] get changed dataId error, code: {}", agent.getName(),
                        result.getCode());
            }
        } catch (Exception e) {
            setHealthServer(false);
            LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception", e);
            throw e;
        }
        return Collections.emptyList();
    }

    /**
     * Get the groupKey list from the http response.
     *
     * @param response Http response.
     * @return GroupKey List, (ps: it maybe null).
     */
    private List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isBlank(response)) {
            return Collections.emptyList();
        }

        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error", e);
        }

        List<String> updateList = new LinkedList<String>();

        // 进行字符串分割解析，将String解析为List
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isBlank(dataIdAndGroup)) {
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 2) {
                    updateList.add(GroupKey.getKey(dataId, group));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}", agent.getName(), dataId,
                            group);
                } else if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}", agent.getName(),
                            dataId, group, tenant);
                } else {
                    LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}", agent.getName(),
                            dataIdAndGroup);
                }
            }
        }
        return updateList;
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final HttpAgent agent, final ConfigFilterChainManager configFilterChainManager,
            final Properties properties) {
        this.agent = agent;
        this.configFilterChainManager = configFilterChainManager;

        // Initialize the timeout parameter

        init(properties);

        // 创建一个线程池，仅包含一个核心线程， 用于执行后面的定时任务
        this.executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
                t.setDaemon(true);
                return t;
            }
        });

        // 创建一个线程池，用于执行后续的长轮询任务，其包含的核心线程数量为当前server的逻辑内核数量
        // ---------- Netty源码中的相关内容 --------------
        // EventLoopGroup  -> 线程池
        // EventLoop -> 线程驱动
        this.executorService = Executors
                .newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
                        t.setDaemon(true);
                        return t;
                    }
                });

        // 执行一个定时任务
        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    // todo 发出配置更新检测请求
                    checkConfigInfo();
                } catch (Throwable e) {
                    LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error", e);
                }
            }
        }, 1L, 10L, TimeUnit.MILLISECONDS);
    }

    private void init(Properties properties) {

        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
                Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);

        taskPenaltyTime = ConvertUtils
                .toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME), Constants.CONFIG_RETRY_TIME);

        this.enableRemoteSyncConfig = Boolean
                .parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, LOGGER);
        ThreadUtils.shutdownThreadPool(executor, LOGGER);
        LOGGER.info("{} do shutdown stop", className);
    }

    class LongPollingRunnable implements Runnable {

        private final int taskId;

        public LongPollingRunnable(int taskId) {
            this.taskId = taskId;
        }

        @Override
        public void run() {

            List<CacheData> cacheDatas = new ArrayList<CacheData>();
            List<String> inInitializingCacheList = new ArrayList<String>();
            try {
                // check failover config
                // 遍历所有配置文件对应的cacheData
                for (CacheData cacheData : cacheMap.values()) {
                    if (cacheData.getTaskId() == taskId) {
                        cacheDatas.add(cacheData);
                        try {
                            // todo 将本地配置文件内容更新到当前cacheData
                            // 本地配置文件，远程配置文件，快照配置文件
                            checkLocalConfig(cacheData);
                            if (cacheData.isUseLocalConfigInfo()) {
                                cacheData.checkListenerMd5();
                            }
                        } catch (Exception e) {
                            LOGGER.error("get local config info error", e);
                        }
                    }
                }

                // check server config
                // todo 从server端检测这些配置文件是否发生了变更
                // 返回结果为所有发生了变更的配置文件的key
                List<String> changedGroupKeys = checkUpdateDataIds(cacheDatas, inInitializingCacheList);
                if (!CollectionUtils.isEmpty(changedGroupKeys)) {
                    LOGGER.info("get changedGroupKeys:" + changedGroupKeys);
                }

                // 遍历所有发生了变更的配置文件key
                for (String groupKey : changedGroupKeys) {
                    String[] key = GroupKey.parseKey(groupKey);
                    String dataId = key[0];
                    String group = key[1];
                    String tenant = null;
                    if (key.length == 3) {
                        tenant = key[2];
                    }
                    try {
                        // todo 从server获取当前配置文件的最新内容
                        String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                        // 获取到当前配置文件的cacheData
                        CacheData cache = cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
                        // 将更新过的内容写入到cacheData
                        cache.setContent(ct[0]);
                        if (null != ct[1]) {
                            cache.setType(ct[1]);
                        }
                        LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                                agent.getName(), dataId, group, tenant, cache.getMd5(),
                                ContentUtils.truncateContent(ct[0]), ct[1]);
                    } catch (NacosException ioe) {
                        String message = String
                                .format("[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s",
                                        agent.getName(), dataId, group, tenant);
                        LOGGER.error(message, ioe);
                    }
                }
                // 收尾工作
                for (CacheData cacheData : cacheDatas) {
                    if (!cacheData.isInitializing() || inInitializingCacheList
                            .contains(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant))) {
                        // todo 监听回调
                        cacheData.checkListenerMd5();
                        cacheData.setInitializing(false);
                    }
                }
                inInitializingCacheList.clear();

                // 启动下次的任务
                executorService.execute(this);

            } catch (Throwable e) {

                // If the rotation training task is abnormal, the next execution time of the task will be punished
                LOGGER.error("longPolling error : ", e);
                executorService.schedule(this, taskPenaltyTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    public boolean isHealthServer() {
        return isHealthServer;
    }

    private void setHealthServer(boolean isHealthServer) {
        this.isHealthServer = isHealthServer;
    }

    final ScheduledExecutorService executor;

    final ScheduledExecutorService executorService;

    /**
     * groupKey -> cacheData.
     */
    private final ConcurrentHashMap<String, CacheData> cacheMap = new ConcurrentHashMap<String, CacheData>();

    private final HttpAgent agent;

    private final ConfigFilterChainManager configFilterChainManager;

    private boolean isHealthServer = true;

    private long timeout;

    private double currentLongingTaskCount = 0;

    private int taskPenaltyTime;

    private boolean enableRemoteSyncConfig = false;
}
