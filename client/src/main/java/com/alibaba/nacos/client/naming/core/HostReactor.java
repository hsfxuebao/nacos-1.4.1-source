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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.beat.BeatReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Host reactor.
 *
 * @author xuanyin
 */
public class HostReactor implements Closeable {

    private static final long DEFAULT_DELAY = 1000L;

    private static final long UPDATE_HOLD_INTERVAL = 5000L;

    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    private final Map<String, ServiceInfo> serviceInfoMap;

    private final Map<String, Object> updatingMap;

    private final PushReceiver pushReceiver;

    private final BeatReactor beatReactor;

    private final NamingProxy serverProxy;

    private final FailoverReactor failoverReactor;

    private final String cacheDir;

    private final boolean pushEmptyProtection;

    private final ScheduledExecutorService executor;

    private final InstancesChangeNotifier notifier;

    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir) {
        this(serverProxy, beatReactor, cacheDir, false, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir, boolean loadCacheAtStart,
            boolean pushEmptyProtection, int pollingThreadCount) {
        // init executorService
        this.executor = new ScheduledThreadPoolExecutor(pollingThreadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });

        this.beatReactor = beatReactor;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }
        this.pushEmptyProtection = pushEmptyProtection;
        this.updatingMap = new ConcurrentHashMap<String, Object>();
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        this.pushReceiver = new PushReceiver(this);
        this.notifier = new InstancesChangeNotifier();

        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        NotifyCenter.registerSubscriber(notifier);
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * subscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void subscribe(String serviceName, String clusters, EventListener eventListener) {
        notifier.registerListener(serviceName, clusters, eventListener);
        getServiceInfo(serviceName, clusters);
    }

    /**
     * unsubscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void unSubscribe(String serviceName, String clusters, EventListener eventListener) {
        notifier.deregisterListener(serviceName, clusters, eventListener);
    }

    public List<ServiceInfo> getSubscribeServices() {
        return notifier.getSubscribeServices();
    }

    /**
     * Process service json.
     *
     * @param json service json
     * @return service info
     */
    // todo 来自Server的数据是最新 数据
    public ServiceInfo processServiceJson(String json) {
        // 转成ServiceInfo类
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        // 从本地注册表中获取对应服务
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());

        if (pushEmptyProtection && !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }

        boolean changed = false;

        // 当前注册表中存在该服务，想办法将来自server端的数据更新到本地注册表中
        if (oldService != null) {

            // 为了安全起见，这种情况几乎是不存在的
            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime() + ", new-t: "
                        + serviceInfo.getLastRefTime());
            }
            // 来自server的serviceInfo替换到注册表中的当前服务
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);

            // 遍历本地注册表中当前服务所有instance实例
            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                // 将当前遍历instance主机的ip:port作为key,instance为value
                // 写入到一个新的map中
                oldHostMap.put(host.toInetAddr(), host);
            }

            // 遍历来自server的当前服务 所有instance实例
            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                // 将当前遍历instance主机的ip:port作为key,instance为value
                // 写入到一个新的map中
                newHostMap.put(host.toInetAddr(), host);
            }

            // 该set集合中存放的是，两个map(oldHostMap与newHostMap)中都有的ip:port，
            // 但它们的instance不相同，此时会将来自于server的instance写入到这个set
            Set<Instance> modHosts = new HashSet<Instance>();
            // 只有newHostMap中存在的instance，即在server端新增的instance
            Set<Instance> newHosts = new HashSet<Instance>();
            // 只有oldHostMap中存在的instance，即在server端被删除的instance
            Set<Instance> remvHosts = new HashSet<Instance>();

            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                    newHostMap.entrySet());
            // 遍历来自于server的主机
            for (Map.Entry<String, Instance> entry : newServiceHosts) {

                Instance host = entry.getValue();
                // ip:port
                String key = entry.getKey();
                // 在注册表中存在该ip:port，但这两个instance又不同，则将这个instance写入到modHosts
                if (oldHostMap.containsKey(key) && !StringUtils
                        .equals(host.toString(), oldHostMap.get(key).toString())) {
                    modHosts.add(host);
                    continue;
                }
                // 若注册表中不存在该ip:port，说明这个主机是新增的，则将其写入到newHosts
                if (!oldHostMap.containsKey(key)) {
                    newHosts.add(host);
                }
            }
            // 遍历来自于本地注册表的主机
            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }
                // 注册表中存在，但来自于server的serviceInfo中不存在，
                // 说明这个instance被干掉了，将其写入到remvHosts
                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }

            }

            if (newHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(newHosts));
            }

            if (remvHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(remvHosts));
            }

            if (modHosts.size() > 0) {
                changed = true;
                // todo 变更心跳信息BeatInfo
                updateBeatInfo(modHosts);
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(modHosts));
            }

            serviceInfo.setJsonFromServer(json);
            // 只要发生了变更，就将这个发生变更的serviceInfo记录到一个缓存队列
            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                        serviceInfo.getClusters(), serviceInfo.getHosts()));
                DiskCache.write(serviceInfo, cacheDir);
            }

        // 本地注册表中没有这个服务，直接将来自Server的serviceInfo写入到本地注册表
        } else {
            changed = true;
            NAMING_LOGGER.info("init new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
            // 将来自于server的serviceInfo写入到注册表
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            // 将这个发生变更的serviceInfo记录到一个缓存队列
            NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            serviceInfo.setJsonFromServer(json);
            DiskCache.write(serviceInfo, cacheDir);
        }

        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());

        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
        }

        return serviceInfo;
    }

    private void updateBeatInfo(Set<Instance> modHosts) {
        for (Instance instance : modHosts) {
            String key = beatReactor.buildKey(instance.getServiceName(), instance.getIp(), instance.getPort());
            if (beatReactor.dom2Beat.containsKey(key) && instance.isEphemeral()) {
                // 构建新的BeatInfo
                BeatInfo beatInfo = beatReactor.buildBeatInfo(instance);
                // 发送心跳
                beatReactor.addBeatInfo(instance.getServiceName(), beatInfo);
            }
        }
    }

    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {

        String key = ServiceInfo.getKey(serviceName, clusters);

        // serviceInfoMap 是client的本地注册表
        // key:groupId@@微服务名称@@clusters名称 value:ServiceInfo
        return serviceInfoMap.get(key);
    }

    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters)
            throws NacosException {
        String result = serverProxy.queryList(serviceName, clusters, 0, false);
        if (StringUtils.isNotEmpty(result)) {
            return JacksonUtils.toObj(result, ServiceInfo.class);
        }
        return null;
    }

    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {

        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        // key的格式为：groupId@@微服务名称@@clusters名称
        String key = ServiceInfo.getKey(serviceName, clusters);
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }

        // todo 从当前Client本地注册表中获取当前服务
        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        // 若本地注册表中没有该服务，则创建一个
        if (null == serviceObj) {
            // 创建一个空的服务（没有任何提供者实例instance的ServiceInfo）
            serviceObj = new ServiceInfo(serviceName, clusters);

            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            // updatingMap 是一个临时缓存，主要使用这个map的key
            // map的key不能重复的特性
            // 只要这个服务名称在这个map中，说明这个服务正在更新中
            updatingMap.put(serviceName, new Object());
            // todo 更新本地注册表ServiceName的服务
            updateServiceNow(serviceName, clusters);
            // 更新完毕，从updatingMap中删除
            updatingMap.remove(serviceName);

        // 若当前注册表中已经有这个服务，那么查看一下临时map下
        // 是否存在该服务，若存在，说明当前服务正在更新中，所以本次操作先等待一段时间，默认5s
        } else if (updatingMap.containsKey(serviceName)) {

            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER
                                .error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }

        // todo 启动一个定时任务，定时更新本地注册表中的当前服务
        scheduleUpdateIfAbsent(serviceName, clusters);

        return serviceInfoMap.get(serviceObj.getKey());
    }

    private void updateServiceNow(String serviceName, String clusters) {
        try {
            // todo
            updateService(serviceName, clusters);
        } catch (NacosException e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        // futureMap是一个缓存map，其key为 groupId@@微服务名称@@clusters
        // value是一个定时异步操作对象
        // 这种结构称之为：双重检测锁，DCL，Double Check Lock
        // 该结构是为了避免在并发情况下，多线程重复写入数据
        // 该结构的特征：
        // 1)有两个不为null的判断
        // 2)有共享集合
        // 3)有synchronized代码块
        if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
            return;
        }

        synchronized (futureMap) {
            if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
                return;
            }
            // 创建一个定时异步操作对象，并启动这个定时任务
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, clusters));
            // 将这个定时异步操作对象写入到缓存map
            futureMap.put(ServiceInfo.getKey(serviceName, clusters), future);
        }
    }

    /**
     * Update service now.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void updateService(String serviceName, String clusters) throws NacosException {
        // 本地注册表中获取当前服务
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {
            // todo 提交get请求，获取服务ServiceInfo
            // 需要注意，返回的是json串
            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);

            if (StringUtils.isNotEmpty(result)) {
                // todo 将来自Server的ServiceInfo更新到本地
                processServiceJson(result);
            }
        } finally {
            if (oldService != null) {
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }

    /**
     * Refresh only.
     *
     * @param serviceName service name
     * @param clusters    cluster
     */
    public void refreshOnly(String serviceName, String clusters) {
        try {
            serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        pushReceiver.shutdown();
        failoverReactor.shutdown();
        NotifyCenter.deregisterSubscriber(notifier);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    public class UpdateTask implements Runnable {

        long lastRefTime = Long.MAX_VALUE;

        private final String clusters;

        private final String serviceName;

        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;

        public UpdateTask(String serviceName, String clusters) {
            this.serviceName = serviceName;
            this.clusters = clusters;
        }

        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }

        private void resetFailCount() {
            failCount = 0;
        }

        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;

            try {
                // 从本地注册表中获取当前服务
                ServiceInfo serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                // 若本地注册表中不存在该服务，则从server获取到后，更新到本地注册表
                if (serviceObj == null) {
                    // 从server获取当前服务，并更新到本地注册表
                    updateService(serviceName, clusters);
                    return;
                }

                // 处理本地注册表中存在当前服务的情况
                // 1)serviceObj.getLastRefTime() 获取到的是当前服务最后被访问的时间，这个时间
                // 是来自于本地注册表的，其记录的是所有提供这个服务的instance中最后一个instance
                // 被访问的时间
                // 2)缓存lastRefTime 记录的是当前instance最后被访问的时间
                // 若1)时间 小于 2)时间，说明当前注册表应该更新的
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    updateService(serviceName, clusters);
                    serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                } else {
                    // if serviceName already updated by push, we should not override it
                    // since the push data may be different from pull through force push
                    refreshOnly(serviceName, clusters);
                }
                // 将来自于注册表的这个最后访问时间更新到当前client的缓存
                lastRefTime = serviceObj.getLastRefTime();

                if (!notifier.isSubscribed(serviceName, clusters) && !futureMap
                        .containsKey(ServiceInfo.getKey(serviceName, clusters))) {
                    // abort the update task
                    NAMING_LOGGER.info("update task is stopped, service:" + serviceName + ", clusters:" + clusters);
                    return;
                }
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                delayTime = serviceObj.getCacheMillis();
                resetFailCount();
            } catch (Throwable e) {
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + serviceName, e);
            } finally {
                // 开启下一次的定时任务
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }
    }
}
