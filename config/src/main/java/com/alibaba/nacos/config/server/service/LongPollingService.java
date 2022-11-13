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

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.config.server.model.SampleResult;
import com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.RequestUtil;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.config.server.utils.LogUtil.MEMORY_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;

/**
 * LongPollingService.
 *
 * @author Nacos
 */
@Service
public class LongPollingService {

    private static final int FIXED_POLLING_INTERVAL_MS = 10000;

    private static final int SAMPLE_PERIOD = 100;

    private static final int SAMPLE_TIMES = 3;

    private static final String TRUE_STR = "true";

    private Map<String, Long> retainIps = new ConcurrentHashMap<String, Long>();

    private static boolean isFixedPolling() {
        return SwitchService.getSwitchBoolean(SwitchService.FIXED_POLLING, false);
    }

    private static int getFixedPollingInterval() {
        return SwitchService.getSwitchInteger(SwitchService.FIXED_POLLING_INTERVAL, FIXED_POLLING_INTERVAL_MS);
    }

    public boolean isClientLongPolling(String clientIp) {
        return getClientPollingRecord(clientIp) != null;
    }

    public Map<String, String> getClientSubConfigInfo(String clientIp) {
        ClientLongPolling record = getClientPollingRecord(clientIp);

        if (record == null) {
            return Collections.<String, String>emptyMap();
        }

        return record.clientMd5Map;
    }

    public SampleResult getSubscribleInfo(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.clientMd5Map.containsKey(groupKey)) {
                lisentersGroupkeyStatus.put(clientLongPolling.ip, clientLongPolling.clientMd5Map.get(groupKey));
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    public SampleResult getSubscribleInfoByIp(String clientIp) {
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // One ip can have multiple listener.
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    /**
     * Aggregate the sampling IP and monitoring configuration information in the sampling results. There is no problem
     * for the merging strategy to cover the previous one with the latter.
     *
     * @param sampleResults sample Results.
     * @return Results.
     */
    public SampleResult mergeSampleResult(List<SampleResult> sampleResults) {
        SampleResult mergeResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);
        for (SampleResult sampleResult : sampleResults) {
            Map<String, String> lisentersGroupkeyStatusTmp = sampleResult.getLisentersGroupkeyStatus();
            for (Map.Entry<String, String> entry : lisentersGroupkeyStatusTmp.entrySet()) {
                lisentersGroupkeyStatus.put(entry.getKey(), entry.getValue());
            }
        }
        mergeResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return mergeResult;
    }

    /**
     * Collect application subscribe configinfos.
     *
     * @return configinfos results.
     */
    public Map<String, Set<String>> collectApplicationSubscribeConfigInfos() {
        if (allSubs == null || allSubs.isEmpty()) {
            return null;
        }
        HashMap<String, Set<String>> app2Groupkeys = new HashMap<String, Set<String>>(50);
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (StringUtils.isEmpty(clientLongPolling.appName) || "unknown"
                    .equalsIgnoreCase(clientLongPolling.appName)) {
                continue;
            }
            Set<String> appSubscribeConfigs = app2Groupkeys.get(clientLongPolling.appName);
            Set<String> clientSubscribeConfigs = clientLongPolling.clientMd5Map.keySet();
            if (appSubscribeConfigs == null) {
                appSubscribeConfigs = new HashSet<String>(clientSubscribeConfigs.size());
            }
            appSubscribeConfigs.addAll(clientSubscribeConfigs);
            app2Groupkeys.put(clientLongPolling.appName, appSubscribeConfigs);
        }

        return app2Groupkeys;
    }

    public SampleResult getCollectSubscribleInfo(String dataId, String group, String tenant) {
        List<SampleResult> sampleResultLst = new ArrayList<SampleResult>(50);
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfo(dataId, group, tenant);
            if (sampleTmp != null) {
                sampleResultLst.add(sampleTmp);
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }

        SampleResult sampleResult = mergeSampleResult(sampleResultLst);
        return sampleResult;
    }

    public SampleResult getCollectSubscribleInfoByIp(String ip) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null && !sampleResult.getLisentersGroupkeyStatus()
                        .equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }
        return sampleResult;
    }

    private ClientLongPolling getClientPollingRecord(String clientIp) {
        if (allSubs == null) {
            return null;
        }

        for (ClientLongPolling clientLongPolling : allSubs) {
            HttpServletRequest request = (HttpServletRequest) clientLongPolling.asyncContext.getRequest();

            if (clientIp.equals(RequestUtil.getRemoteIp(request))) {
                return clientLongPolling;
            }
        }

        return null;
    }

    /**
     * Add LongPollingClient.
     *
     * @param req              HttpServletRequest.
     * @param rsp              HttpServletResponse.
     * @param clientMd5Map     clientMd5Map.
     * @param probeRequestSize probeRequestSize.
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
            int probeRequestSize) {

        // 从请求中获取长连接维护的时长
        String str = req.getHeader(LongPollingService.LONG_POLLING_HEADER);
        // 从请求中获取长连接是否不进行挂起（boolean），该属性是针对 非固定时长长轮询 的
        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);
        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        String tag = req.getHeader("Vipserver-Tag");
        // 这个时间是长连接提前执行关闭任务的时间，即长连接真正维护的时长是29.5s
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);

        // Add delay time for LoadBalance, and one response is returned 500 ms in advance to avoid client timeout.
        // 是长连接真正维护的时长，29.5s
        long timeout = Math.max(10000, Long.parseLong(str) - delayTime);
        // 处理固定时长的长轮询情况：仅获取了用户指定的长连接的时长
        if (isFixedPolling()) {
            timeout = Math.max(10000, getFixedPollingInterval());
            // Do nothing but set fix polling timeout.
        // 处理非固定时长的长轮询情况
        } else {
            long start = System.currentTimeMillis();
            // todo 获取目标配置中所有发生了配置变更的文件key
            List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
            // 处理有配置变更发生的情况
            if (changedGroups.size() > 0) {
                // 生成response，并关闭连接
                generateResponse(req, rsp, changedGroups);
                LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "instant",
                        RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                        changedGroups.size());
                return;
            // 处理没有配置变更发生，且非固定时长长连接不挂起的情况：直接关闭连接
            } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
                LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup",
                        RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                        changedGroups.size());
                return;
            }
        }
        // 在哪种情况下代码会走到这里？
        // 1）固定时长的长轮询
        // 2）挂起的非固定时长的长轮询

        // 从请求中获取提交请求的config client的ip
        String ip = RequestUtil.getRemoteIp(req);

        // Must be called by http thread, or send response.
        final AsyncContext asyncContext = req.startAsync();

        // AsyncContext.setTimeout() is incorrect, Control by oneself
        asyncContext.setTimeout(0L);

        // todo 立即执行客户端长轮询任务ClientLongPolling
        ConfigExecutor.executeLongPolling(
                new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }

    public static boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        // 创建allSubs队列。该队列中存放的是当前server所hold的所有长轮询实例
        allSubs = new ConcurrentLinkedQueue<ClientLongPolling>();

        // 定义并启动一个定时任务，用于进行长轮询统计
        ConfigExecutor.scheduleLongPolling(new StatTask(), 0L, 10L, TimeUnit.SECONDS);

        // Register LocalDataChangeEvent to NotifyCenter.
        // 将LocalDataChangeEvent注册到NotifyCenter，这样的话，
        // 注册到NotifyCenter的订阅者就会被LocalDataChangeEvent事件给触发
        NotifyCenter.registerToPublisher(LocalDataChangeEvent.class, NotifyCenter.ringBufferSize);

        // Register A Subscriber to subscribe LocalDataChangeEvent.
        // 向NotifyCenter注册一个LocalDataChangeEvent事件的订阅者
        NotifyCenter.registerSubscriber(new Subscriber() {

            // 回调方法：一旦发生了数据变更事件，就会触发该方法的执行
            @Override
            public void onEvent(Event event) {
                // 若当前长连接为固定时长的，那么忽略本次变更
                if (isFixedPolling()) {
                    // Ignore.

                // 若当前长连接为挂起的非固定时长的，则进行相关处理~
                } else {
                    if (event instanceof LocalDataChangeEvent) {
                        LocalDataChangeEvent evt = (LocalDataChangeEvent) event;
                        // 立即执行DataChangeTask任务
                        ConfigExecutor.executeLongPolling(new DataChangeTask(evt.groupKey, evt.isBeta, evt.betaIps));
                    }
                }
            }

            @Override
            public Class<? extends Event> subscribeType() {
                return LocalDataChangeEvent.class;
            }
        });

    }

    public static final String LONG_POLLING_HEADER = "Long-Pulling-Timeout";

    public static final String LONG_POLLING_NO_HANG_UP_HEADER = "Long-Pulling-Timeout-No-Hangup";

    /**
     * ClientLongPolling subscibers.
     */
    final Queue<ClientLongPolling> allSubs;

    class DataChangeTask implements Runnable {

        @Override
        public void run() {
            try {
                ConfigCacheService.getContentBetaMd5(groupKey);
                for (Iterator<ClientLongPolling> iter = allSubs.iterator(); iter.hasNext(); ) {
                    ClientLongPolling clientSub = iter.next();
                    if (clientSub.clientMd5Map.containsKey(groupKey)) {
                        // If published tag is not in the beta list, then it skipped.
                        if (isBeta && !CollectionUtils.contains(betaIps, clientSub.ip)) {
                            continue;
                        }

                        // If published tag is not in the tag list, then it skipped.
                        if (StringUtils.isNotBlank(tag) && !tag.equals(clientSub.tag)) {
                            continue;
                        }

                        getRetainIps().put(clientSub.ip, System.currentTimeMillis());
                        iter.remove(); // Delete subscribers' relationships.
                        LogUtil.CLIENT_LOG
                                .info("{}|{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - changeTime), "in-advance",
                                        RequestUtil
                                                .getRemoteIp((HttpServletRequest) clientSub.asyncContext.getRequest()),
                                        "polling", clientSub.clientMd5Map.size(), clientSub.probeRequestSize, groupKey);
                        clientSub.sendResponse(Arrays.asList(groupKey));
                    }
                }
            } catch (Throwable t) {
                LogUtil.DEFAULT_LOG.error("data change error: {}", ExceptionUtil.getStackTrace(t));
            }
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps) {
            this(groupKey, isBeta, betaIps, null);
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
            this.groupKey = groupKey;
            this.isBeta = isBeta;
            this.betaIps = betaIps;
            this.tag = tag;
        }

        final String groupKey;

        final long changeTime = System.currentTimeMillis();

        final boolean isBeta;

        final List<String> betaIps;

        final String tag;
    }

    class StatTask implements Runnable {

        @Override
        public void run() {
            MEMORY_LOG.info("[long-pulling] client count " + allSubs.size());
            MetricsMonitor.getLongPollingMonitor().set(allSubs.size());
        }
    }

    class ClientLongPolling implements Runnable {

        @Override
        public void run() { // 外层run()
            // 定义并执行一个异步定时任务
            asyncTimeoutFuture = ConfigExecutor.scheduleLongPolling(new Runnable() {
                // 29.5s后会执行这个内层run()
                @Override
                public void run() {  // 内层run()
                    try {
                        // retainIps是一个缓存map，其中存放着当前server所hold的
                        // 所有长轮询实例对应的client最近访问server的时间
                        getRetainIps().put(ClientLongPolling.this.ip, System.currentTimeMillis());

                        // Delete subsciber's relations.
                        // 将当前长轮询实例从allSubs中删除，因为当前长连接马上就要关闭了
                        allSubs.remove(ClientLongPolling.this);

                        // 处理固定时长长轮询的情况
                        if (isFixedPolling()) {
                            LogUtil.CLIENT_LOG
                                    .info("{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - createTime), "fix",
                                            RequestUtil.getRemoteIp((HttpServletRequest) asyncContext.getRequest()),
                                            "polling", clientMd5Map.size(), probeRequestSize);
                            // 获取目标配置中所有发生了配置变更的文件key
                            List<String> changedGroups = MD5Util
                                    .compareMd5((HttpServletRequest) asyncContext.getRequest(),
                                            (HttpServletResponse) asyncContext.getResponse(), clientMd5Map);
                            // 将结果返回
                            if (changedGroups.size() > 0) {
                                sendResponse(changedGroups);
                            } else {
                                sendResponse(null);
                            }
                        // 处理挂起的非固定时长长轮询的情况：向client返回一个空的响应，并关闭连接
                        } else {
                            LogUtil.CLIENT_LOG
                                    .info("{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - createTime), "timeout",
                                            RequestUtil.getRemoteIp((HttpServletRequest) asyncContext.getRequest()),
                                            "polling", clientMd5Map.size(), probeRequestSize);
                            // todo
                            sendResponse(null);
                        }
                    } catch (Throwable t) {
                        LogUtil.DEFAULT_LOG.error("long polling error:" + t.getMessage(), t.getCause());
                    }

                }  // end-内层run()

            }, timeoutTime, TimeUnit.MILLISECONDS);
            // allSubs是一个缓存队列，其中存放着当前config server所hold的所有长轮询实例
            // 将当前长轮询实例写入到缓存队列
            allSubs.add(this);
        } // end-外层run()

        void sendResponse(List<String> changedGroups) {

            // Cancel time out task.
            if (null != asyncTimeoutFuture) {
                asyncTimeoutFuture.cancel(false);
            }
            // todo 生成response
            generateResponse(changedGroups);
        }

        void generateResponse(List<String> changedGroups) {
            if (null == changedGroups) {

                // Tell web container to send http response.
                asyncContext.complete();
                return;
            }

            HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

            try {
                // 将list转换为string
                final String respString = MD5Util.compareMd5ResultString(changedGroups);

                // Disable cache.
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println(respString);
                // 告诉web容器去发送http响应
                asyncContext.complete();
            } catch (Exception ex) {
                PULL_LOG.error(ex.toString(), ex);
                asyncContext.complete();
            }
        }

        ClientLongPolling(AsyncContext ac, Map<String, String> clientMd5Map, String ip, int probeRequestSize,
                long timeoutTime, String appName, String tag) {
            this.asyncContext = ac;
            this.clientMd5Map = clientMd5Map;
            this.probeRequestSize = probeRequestSize;
            this.createTime = System.currentTimeMillis();
            this.ip = ip;
            this.timeoutTime = timeoutTime;
            this.appName = appName;
            this.tag = tag;
        }

        final AsyncContext asyncContext;

        final Map<String, String> clientMd5Map;

        final long createTime;

        final String ip;

        final String appName;

        final String tag;

        final int probeRequestSize;

        final long timeoutTime;

        Future<?> asyncTimeoutFuture;

        @Override
        public String toString() {
            return "ClientLongPolling{" + "clientMd5Map=" + clientMd5Map + ", createTime=" + createTime + ", ip='" + ip
                    + '\'' + ", appName='" + appName + '\'' + ", tag='" + tag + '\'' + ", probeRequestSize="
                    + probeRequestSize + ", timeoutTime=" + timeoutTime + '}';
        }
    }

    void generateResponse(HttpServletRequest request, HttpServletResponse response, List<String> changedGroups) {
        if (null == changedGroups) {
            return;
        }

        try {
            final String respString = MD5Util.compareMd5ResultString(changedGroups);
            // Disable cache.
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(respString);
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }

    public Map<String, Long> getRetainIps() {
        return retainIps;
    }

    public void setRetainIps(Map<String, Long> retainIps) {
        this.retainIps = retainIps;
    }
}
