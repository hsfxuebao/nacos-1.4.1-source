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

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thread to update ephemeral instance triggered by client beat.
 *
 * @author nkorange
 */
public class ClientBeatProcessor implements Runnable {

    public static final long CLIENT_BEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    private RsInfo rsInfo;

    private Service service;

    @JsonIgnore
    public PushService getPushService() {
        return ApplicationUtils.getBean(PushService.class);
    }

    public RsInfo getRsInfo() {
        return rsInfo;
    }

    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public void run() {
        Service service = this.service;
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }

        String ip = rsInfo.getIp();
        String clusterName = rsInfo.getCluster();
        int port = rsInfo.getPort();
        Cluster cluster = service.getClusterMap().get(clusterName);
        // 获取当前服务的所有临时实例
        List<Instance> instances = cluster.allIPs(true);
        // 遍历所有这些临时实例，从中查找当前发送心跳的instance
        for (Instance instance : instances) {
            // 只要ip与port与当前心跳的instance的相同，就是了
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                // 修改最后心跳时间戳
                instance.setLastBeat(System.currentTimeMillis());
                // 修改该instance的健康状态
                // 当instance被标记时，即其marked为true时，其是一个持久实例
                if (!instance.isMarked()) {
                    // instance的healthy才是临时实例健康状态的表示
                    // 若当前instance健康状态为false，但本次是其发送的心跳，说明这个instance“起死回生”了，
                    // 我们需要将其health变为true
                    if (!instance.isHealthy()) {
                        instance.setHealthy(true);
                        Loggers.EVT_LOG
                                .info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                                        cluster.getService().getName(), ip, port, cluster.getName(),
                                        UtilsAndCommons.LOCALHOST_SITE);
                        // todo 发布服务变更事件（其对后续我们要分析的UDP通信非常重要）
                        getPushService().serviceChanged(service);
                    }
                }
            }
        }
    }
}
