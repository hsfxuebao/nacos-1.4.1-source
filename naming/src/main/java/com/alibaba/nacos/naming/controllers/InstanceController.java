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

package com.alibaba.nacos.naming.controllers;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.common.ActionTypes;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.SwitchEntry;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationContext;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.push.ClientInfo;
import com.alibaba.nacos.naming.push.DataSource;
import com.alibaba.nacos.naming.push.PushService;
import com.alibaba.nacos.naming.web.CanDistro;
import com.alibaba.nacos.naming.web.NamingResourceParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.DEFAULT_CLUSTER_NAME;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.EPHEMERAL;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.PERSIST;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_REMOVE;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_UPDATE;

/**
 * Instance operation controller.
 *
 * @author nkorange
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance")
public class InstanceController {

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private PushService pushService;

    @Autowired
    private ServiceManager serviceManager;

    private DataSource pushDataSource = new DataSource() {

        @Override
        public String getData(PushService.PushClient client) {

            ObjectNode result = JacksonUtils.createEmptyJsonNode();
            try {
                result = doSrvIpxt(client.getNamespaceId(), client.getServiceName(), client.getAgent(),
                        client.getClusters(), client.getSocketAddr().getAddress().getHostAddress(), 0,
                        StringUtils.EMPTY, false, StringUtils.EMPTY, StringUtils.EMPTY, false);
            } catch (Exception e) {
                Loggers.SRV_LOG.warn("PUSH-SERVICE: service is not modified", e);
            }

            // overdrive the cache millis to push mode
            result.put("cacheMillis", switchDomain.getPushCacheMillis(client.getServiceName()));

            return result.toString();
        }
    };

    /**
     * Register new instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during register
     */
    @CanDistro
    @PostMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String register(HttpServletRequest request) throws Exception {
        // 从请求中获取指定属性值
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        // 从请求中获取指定属性值
        final String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        // 检测serviceName是否合法
        NamingUtils.checkServiceNameFormat(serviceName);

        // 通过请求参数组装出instance
        final Instance instance = parseInstance(request);

        // todo 将instance写到注册表
        serviceManager.registerInstance(namespaceId, serviceName, instance);
        return "ok";
    }

    /**
     * Deregister instances.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during deregister
     */
    @CanDistro
    @DeleteMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String deregister(HttpServletRequest request) throws Exception {
        // 从请求中获取要操作的instance
        Instance instance = getIpAddress(request);
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        // 从注册表中获取service
        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", serviceName);
            return "ok";
        }
        // todo 删除instance
        serviceManager.removeInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
        return "ok";
    }

    /**
     * Update instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during update
     */
    @CanDistro
    @PutMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String update(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        final String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        final Instance instance = parseInstance(request);

        String agent = WebUtils.getUserAgent(request);

        ClientInfo clientInfo = new ClientInfo(agent);

        if (clientInfo.type == ClientInfo.ClientType.JAVA
                && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
            serviceManager.updateInstance(namespaceId, serviceName, instance);
        } else {
            serviceManager.registerInstance(namespaceId, serviceName, instance);
        }
        return "ok";
    }

    /**
     * Batch update instance's metadata. old key exist = update, old key not exist = add.
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @PutMapping(value = "/metadata/batch")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode batchUpdateInstanceMatadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);

        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);

        List<Instance> targetInstances = parseBatchInstances(instances);

        String metadata = WebUtils.required(request, "metadata");
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);

        List<Instance> operatedInstances = batchOperateMetadata(namespaceId,
                buildOperationInfo(serviceName, consistencyType, targetInstances), targetMetadata,
                UPDATE_INSTANCE_METADATA_ACTION_UPDATE);

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : operatedInstances) {
            ipArray.add(ip.getDatumKey() + ":" + (ip.isEphemeral() ? EPHEMERAL : PERSIST));
        }

        result.replace("updated", ipArray);
        return result;
    }

    /**
     * Batch delete instance's metadata. old key exist = delete, old key not exist = not operate
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @DeleteMapping("/metadata/batch")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode batchDeleteInstanceMatadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);

        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);

        List<Instance> targetInstances = parseBatchInstances(instances);

        String metadata = WebUtils.required(request, "metadata");
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);

        List<Instance> operatedInstances = batchOperateMetadata(namespaceId,
                buildOperationInfo(serviceName, consistencyType, targetInstances), targetMetadata,
                UPDATE_INSTANCE_METADATA_ACTION_REMOVE);

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : operatedInstances) {
            ipArray.add(ip.getDatumKey() + ":" + (ip.isEphemeral() ? EPHEMERAL : PERSIST));
        }

        result.replace("updated", ipArray);
        return result;
    }

    private InstanceOperationInfo buildOperationInfo(String serviceName, String consistencyType,
            List<Instance> instances) {
        if (!CollectionUtils.isEmpty(instances)) {
            for (Instance instance : instances) {
                if (StringUtils.isBlank(instance.getClusterName())) {
                    instance.setClusterName(DEFAULT_CLUSTER_NAME);
                }
            }
        }
        return new InstanceOperationInfo(serviceName, consistencyType, instances);
    }

    private List<Instance> parseBatchInstances(String instances) {
        try {
            return JacksonUtils.toObj(instances, new TypeReference<List<Instance>>() {
            });
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("UPDATE-METADATA: Param 'instances' is illegal, ignore this operation", e);
        }
        return null;
    }

    private List<Instance> batchOperateMetadata(String namespace, InstanceOperationInfo instanceOperationInfo,
            Map<String, String> metadata, String action) {
        Function<InstanceOperationContext, List<Instance>> operateFunction = instanceOperationContext -> {
            try {
                return serviceManager.updateMetadata(instanceOperationContext.getNamespace(),
                        instanceOperationContext.getServiceName(), instanceOperationContext.getEphemeral(), action,
                        instanceOperationContext.getAll(), instanceOperationContext.getInstances(), metadata);
            } catch (NacosException e) {
                Loggers.SRV_LOG.warn("UPDATE-METADATA: updateMetadata failed", e);
            }
            return new ArrayList<>();
        };
        return serviceManager.batchOperate(namespace, instanceOperationInfo, operateFunction);
    }

    /**
     * Patch instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during patch
     */
    @CanDistro
    @PatchMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String patch(HttpServletRequest request) throws Exception {
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String ip = WebUtils.required(request, "ip");
        String port = WebUtils.required(request, "port");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }

        Instance instance = serviceManager.getInstance(namespaceId, serviceName, cluster, ip, Integer.parseInt(port));
        if (instance == null) {
            throw new IllegalArgumentException("instance not found");
        }

        String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(metadata)) {
            instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }
        String app = WebUtils.optional(request, "app", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(app)) {
            instance.setApp(app);
        }
        String weight = WebUtils.optional(request, "weight", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(weight)) {
            instance.setWeight(Double.parseDouble(weight));
        }
        String healthy = WebUtils.optional(request, "healthy", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(healthy)) {
            instance.setHealthy(BooleanUtils.toBoolean(healthy));
        }
        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(enabledString)) {
            instance.setEnabled(BooleanUtils.toBoolean(enabledString));
        }
        instance.setLastBeat(System.currentTimeMillis());
        instance.validate();
        serviceManager.updateInstance(namespaceId, serviceName, instance);
        return "ok";
    }

    /**
     * Get all instance of input service.
     *
     * @param request http request
     * @return list of instance
     * @throws Exception any error during list
     */
    @GetMapping("/list")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.READ)
    public ObjectNode list(HttpServletRequest request) throws Exception {
        // 从请求中获取各种属性
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        // agent属性用于指定提交请求的客户端是哪种类型
        String agent = WebUtils.getUserAgent(request);
        String clusters = WebUtils.optional(request, "clusters", StringUtils.EMPTY);
        String clientIP = WebUtils.optional(request, "clientIP", StringUtils.EMPTY);
        // 获取到client的端口号，后续UDP通信会使用
        int udpPort = Integer.parseInt(WebUtils.optional(request, "udpPort", "0"));
        String env = WebUtils.optional(request, "env", StringUtils.EMPTY);
        boolean isCheck = Boolean.parseBoolean(WebUtils.optional(request, "isCheck", "false"));

        String app = WebUtils.optional(request, "app", StringUtils.EMPTY);

        String tenant = WebUtils.optional(request, "tid", StringUtils.EMPTY);

        boolean healthyOnly = Boolean.parseBoolean(WebUtils.optional(request, "healthyOnly", "false"));
        // todo 对请求进行详细处理
        return doSrvIpxt(namespaceId, serviceName, agent, clusters, clientIP, udpPort, env, isCheck, app, tenant,
                healthyOnly);
    }

    /**
     * Get detail information of specified instance.
     *
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during get
     */
    @GetMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.READ)
    public ObjectNode detail(HttpServletRequest request) throws Exception {

        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        String ip = WebUtils.required(request, "ip");
        int port = Integer.parseInt(WebUtils.required(request, "port"));

        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "no service " + serviceName + " found!");
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            throw new NacosException(NacosException.NOT_FOUND,
                    "no ips found for cluster " + cluster + " in service " + serviceName);
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                ObjectNode result = JacksonUtils.createEmptyJsonNode();
                result.put("service", serviceName);
                result.put("ip", ip);
                result.put("port", port);
                result.put("clusterName", cluster);
                result.put("weight", instance.getWeight());
                result.put("healthy", instance.isHealthy());
                result.put("instanceId", instance.getInstanceId());
                result.set("metadata", JacksonUtils.transferToJsonNode(instance.getMetadata()));
                return result;
            }
        }

        throw new NacosException(NacosException.NOT_FOUND, "no matched ip found!");
    }

    /**
     * Create a beat for instance.
     *
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during handle
     */
    @CanDistro
    @PutMapping("/beat")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode beat(HttpServletRequest request) throws Exception {
        // 创建一个JSON Node，该方法的返回值就是它，后面的代码就是对这个Node进行各种初始化
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        result.put(SwitchEntry.CLIENT_BEAT_INTERVAL, switchDomain.getClientBeatInterval());

        // 从请求中获取到beat，即client端的beatInfo
        String beat = WebUtils.optional(request, "beat", StringUtils.EMPTY);
        RsInfo clientBeat = null;
        // 将beat构建为clientBeat
        if (StringUtils.isNotBlank(beat)) {
            clientBeat = JacksonUtils.toObj(beat, RsInfo.class);
        }
        String clusterName = WebUtils
                .optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        String ip = WebUtils.optional(request, "ip", StringUtils.EMPTY);
        // 获取到客户端传递来的client的port，其将来用于UDP通信
        int port = Integer.parseInt(WebUtils.optional(request, "port", "0"));
        if (clientBeat != null) {
            if (StringUtils.isNotBlank(clientBeat.getCluster())) {
                clusterName = clientBeat.getCluster();
            } else {
                // fix #2533
                clientBeat.setCluster(clusterName);
            }
            ip = clientBeat.getIp();
            port = clientBeat.getPort();
        }
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        Loggers.SRV_LOG.debug("[CLIENT-BEAT] full arguments: beat: {}, serviceName: {}", clientBeat, serviceName);
        // 从注册表中获取当前发送请求的client对应的instance
        Instance instance = serviceManager.getInstance(namespaceId, serviceName, clusterName, ip, port);
        // 处理注册表中不存在该client的instance的情况
        if (instance == null) {
            // 若请求中没有携带心跳数据，则直接返回
            if (clientBeat == null) {
                result.put(CommonParams.CODE, NamingResponseCode.RESOURCE_NOT_FOUND);
                return result;
            }

            Loggers.SRV_LOG.warn("[CLIENT-BEAT] The instance has been removed for health mechanism, "
                    + "perform data compensation operations, beat: {}, serviceName: {}", clientBeat, serviceName);
            // 下面处理的情况是，注册表中没有该client的instance，但其发送的请求中具有心跳数据。
            // 在client的注册请求还未到达时（网络抖动等原因），第一次心跳请求先到达了server，会出现这种情况
            // 处理方式是，使用心跳数据构建出一个instance，注册到注册表
            instance = new Instance();
            instance.setPort(clientBeat.getPort());
            instance.setIp(clientBeat.getIp());
            instance.setWeight(clientBeat.getWeight());
            instance.setMetadata(clientBeat.getMetadata());
            instance.setClusterName(clusterName);
            instance.setServiceName(serviceName);
            instance.setInstanceId(instance.getInstanceId());
            instance.setEphemeral(clientBeat.isEphemeral());
            // 注册
            serviceManager.registerInstance(namespaceId, serviceName, instance);
        }
        // 从注册表中获取service
        Service service = serviceManager.getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.SERVER_ERROR,
                    "service not found: " + serviceName + "@" + namespaceId);
        }
        if (clientBeat == null) {
            clientBeat = new RsInfo();
            clientBeat.setIp(ip);
            clientBeat.setPort(port);
            clientBeat.setCluster(clusterName);
        }
        // todo 处理本次心跳
        service.processClientBeat(clientBeat);

        result.put(CommonParams.CODE, NamingResponseCode.OK);
        // 这个就有点动态配置了
        // 如果instance中有 preserved.heart.beat.interval 这个参数
        if (instance.containsMetadata(PreservedMetadataKeys.HEART_BEAT_INTERVAL)) {
            // 就带回给客户端
            result.put(SwitchEntry.CLIENT_BEAT_INTERVAL, instance.getInstanceHeartBeatInterval());
        }
        result.put(SwitchEntry.LIGHT_BEAT_ENABLED, switchDomain.isLightBeatEnabled());
        return result;
    }

    /**
     * List all instance with health status.
     *
     * @param key (namespace##)?serviceName
     * @return list of instance
     * @throws NacosException any error during handle
     */
    @RequestMapping("/statuses")
    public ObjectNode listWithHealthStatus(@RequestParam String key) throws NacosException {

        String serviceName;
        String namespaceId;

        if (key.contains(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)) {
            namespaceId = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[0];
            serviceName = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
        } else {
            namespaceId = Constants.DEFAULT_NAMESPACE_ID;
            serviceName = key;
        }
        NamingUtils.checkServiceNameFormat(serviceName);
        Service service = serviceManager.getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "service: " + serviceName + " not found.");
        }

        List<Instance> ips = service.allIPs();

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : ips) {
            ipArray.add(ip.toIpAddr() + "_" + ip.isHealthy());
        }

        result.replace("ips", ipArray);
        return result;
    }

    private Instance parseInstance(HttpServletRequest request) throws Exception {

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String app = WebUtils.optional(request, "app", "DEFAULT");
        // 通过请求中的数据 组装出一个instance
        Instance instance = getIpAddress(request);
        // 初始化instance
        instance.setApp(app);
        instance.setServiceName(serviceName);
        // Generate simple instance id first. This value would be updated according to
        // INSTANCE_ID_GENERATOR.
        instance.setInstanceId(instance.generateInstanceId());
        instance.setLastBeat(System.currentTimeMillis());
        String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);
        if (StringUtils.isNotEmpty(metadata)) {
            instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }

        instance.validate();

        return instance;
    }

    private Instance getIpAddress(HttpServletRequest request) {

        // 从请求中 获取各种属性值
        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        boolean enabled;
        if (StringUtils.isBlank(enabledString)) {
            enabled = BooleanUtils.toBoolean(WebUtils.optional(request, "enable", "true"));
        } else {
            enabled = BooleanUtils.toBoolean(enabledString);
        }

        String weight = WebUtils.optional(request, "weight", "1");
        boolean healthy = BooleanUtils.toBoolean(WebUtils.optional(request, "healthy", "true"));

        Instance instance = getBasicIpAddress(request);
        instance.setWeight(Double.parseDouble(weight));
        instance.setHealthy(healthy);
        instance.setEnabled(enabled);

        return instance;
    }

    private Instance getBasicIpAddress(HttpServletRequest request) {

        final String ip = WebUtils.required(request, "ip");
        final String port = WebUtils.required(request, "port");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }
        boolean ephemeral = BooleanUtils.toBoolean(
                WebUtils.optional(request, "ephemeral", String.valueOf(switchDomain.isDefaultInstanceEphemeral())));

        Instance instance = new Instance();
        instance.setPort(Integer.parseInt(port));
        instance.setIp(ip);
        instance.setEphemeral(ephemeral);
        instance.setClusterName(cluster);

        return instance;
    }

    private void checkIfDisabled(Service service) throws Exception {
        if (!service.getEnabled()) {
            throw new Exception("service is disabled now.");
        }
    }

    /**
     * Get service full information with instances.
     *
     * @param namespaceId namespace id
     * @param serviceName service name
     * @param agent       agent infor string
     * @param clusters    cluster names
     * @param clientIP    client ip
     * @param udpPort     push udp port
     * @param env         env
     * @param isCheck     is check request
     * @param app         app name
     * @param tid         tenant
     * @param healthyOnly whether only for healthy check
     * @return service full information with instances
     * @throws Exception any error during handle
     */
    public ObjectNode doSrvIpxt(String namespaceId, String serviceName, String agent, String clusters, String clientIP,
            int udpPort, String env, boolean isCheck, String app, String tid, boolean healthyOnly) throws Exception {
        // 不同agent，生成不同的clientInfo
        ClientInfo clientInfo = new ClientInfo(agent);
        // 创建一个JSON Node，其就是当前方法返回的结果。后续代码就是对这个Node的各种初始化
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        // 从注册表中获取当前服务
        Service service = serviceManager.getService(namespaceId, serviceName);
        long cacheMillis = switchDomain.getDefaultCacheMillis();

        // now try to enable the push
        try {
            // udpPort大于0，&& 客户端语言版本判断，看能不能UDP推送
            if (udpPort > 0 && pushService.canEnablePush(agent)) {
                // 创建当前发出订阅请求的Nacos client的UDP Client
                // 注意，在Nacos的UDP通信中，Nacos Server充当的是UDP Client，Nacos Client充当的是UDP Server
                pushService
                        .addClient(namespaceId, serviceName, clusters, agent, new InetSocketAddress(clientIP, udpPort),
                                pushDataSource, tid, app);
                cacheMillis = switchDomain.getPushCacheMillis(serviceName);
            }
        } catch (Exception e) {
            Loggers.SRV_LOG
                    .error("[NACOS-API] failed to added push client {}, {}:{}", clientInfo, clientIP, udpPort, e);
            cacheMillis = switchDomain.getDefaultCacheMillis();
        }

        if (service == null) {
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }
            result.put("name", serviceName);
            result.put("clusters", clusters);
            result.put("cacheMillis", cacheMillis);
            result.replace("hosts", JacksonUtils.createEmptyArrayNode());
            return result;
        }
        // 代码直到这里，说明注册表中存在该服务
        // 检测该服务是否被禁。若是被禁的服务，直接抛出异常
        checkIfDisabled(service);

        List<Instance> srvedIPs;
        // 获取到当前服务的所有实例，包含所有持久/临时实例
        srvedIPs = service.srvIPs(Arrays.asList(StringUtils.split(clusters, ",")));

        // filter ips using selector:
        // 若选择器不空，则根据选择算法选择可用的intance列表，默认情况下，选择器不做任务过滤
        if (service.getSelector() != null && StringUtils.isNotBlank(clientIP)) {
            srvedIPs = service.getSelector().select(clientIP, srvedIPs);
        }
        // 若最终选择的结果为空，则直接结束
        if (CollectionUtils.isEmpty(srvedIPs)) {

            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }

            if (clientInfo.type == ClientInfo.ClientType.JAVA
                    && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
                result.put("dom", serviceName);
            } else {
                result.put("dom", NamingUtils.getServiceName(serviceName));
            }

            result.put("name", serviceName);
            result.put("cacheMillis", cacheMillis);
            result.put("lastRefTime", System.currentTimeMillis());
            result.put("checksum", service.getChecksum());
            result.put("useSpecifiedURL", false);
            result.put("clusters", clusters);
            result.put("env", env);
            // 注意，hosts为空
            result.set("hosts", JacksonUtils.createEmptyArrayNode());
            result.set("metadata", JacksonUtils.transferToJsonNode(service.getMetadata()));
            return result;
        }
        // 代码走到这里，说明具有可用的instance
        Map<Boolean, List<Instance>> ipMap = new HashMap<>(2);
        // 这个map只有两个key，True与False
        // key为true的value中存放的是所有健康的instance
        // key为false的value存放的是所有不健康的instance
        ipMap.put(Boolean.TRUE, new ArrayList<>());
        ipMap.put(Boolean.FALSE, new ArrayList<>());
        // 根据instance的健康状态，将所有instance分流放入map的不同key的value中
        for (Instance ip : srvedIPs) {
            // 这个语句写的非常好
            ipMap.get(ip.isHealthy()).add(ip);
        }
        // isCheck为true，表示需要检测instance的保护阈值
        if (isCheck) {
            // reachProtectThreshold 是否达到保护阈值
            result.put("reachProtectThreshold", false);
        }
        // 获取服务的保护阈值
        double threshold = service.getProtectThreshold();
        // 若  "健康instance数量/instance总数" <= 保护阈值，则说明需要启动保护机制了
        if ((float) ipMap.get(Boolean.TRUE).size() / srvedIPs.size() <= threshold) {

            Loggers.SRV_LOG.warn("protect threshold reached, return all ips, service: {}", serviceName);
            if (isCheck) {
                result.put("reachProtectThreshold", true);
            }
            // 将所有不健康的instance添加到的key为true的instance列表，
            // 即key为true的value中（instance列表）存放的是所有instance实例
            // 包含所有健康的与不健康的instance
            ipMap.get(Boolean.TRUE).addAll(ipMap.get(Boolean.FALSE));
            // 清空key为false的value（不健康的instance列表）
            ipMap.get(Boolean.FALSE).clear();
        }

        if (isCheck) {
            result.put("protectThreshold", service.getProtectThreshold());
            result.put("reachLocalSiteCallThreshold", false);

            return JacksonUtils.createEmptyJsonNode();
        }

        ArrayNode hosts = JacksonUtils.createEmptyArrayNode();
        // 注意，这个ipMap中存放着所有健康与不健康的instance列表
        for (Map.Entry<Boolean, List<Instance>> entry : ipMap.entrySet()) {
            List<Instance> ips = entry.getValue();
            // 若客户端只要健康的instance，且当前遍历的map的key为false，则跳过
            if (healthyOnly && !entry.getKey()) {
                continue;
            }
            // 遍历的这个ips可能是所有不健康的instance列表，
            // 也可能是所有健康的instance列表，
            // 也可能是所有健康与不健康的instance列表总和
            for (Instance instance : ips) {

                // remove disabled instance:
                // 跳过禁用的instance
                if (!instance.isEnabled()) {
                    continue;
                }

                ObjectNode ipObj = JacksonUtils.createEmptyJsonNode();
                // 将当前遍历的instance转换为JSON
                ipObj.put("ip", instance.getIp());
                ipObj.put("port", instance.getPort());
                // deprecated since nacos 1.0.0:
                ipObj.put("valid", entry.getKey());
                ipObj.put("healthy", entry.getKey());
                ipObj.put("marked", instance.isMarked());
                ipObj.put("instanceId", instance.getInstanceId());
                ipObj.set("metadata", JacksonUtils.transferToJsonNode(instance.getMetadata()));
                ipObj.put("enabled", instance.isEnabled());
                ipObj.put("weight", instance.getWeight());
                ipObj.put("clusterName", instance.getClusterName());
                if (clientInfo.type == ClientInfo.ClientType.JAVA
                        && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
                    ipObj.put("serviceName", instance.getServiceName());
                } else {
                    ipObj.put("serviceName", NamingUtils.getServiceName(instance.getServiceName()));
                }

                ipObj.put("ephemeral", instance.isEphemeral());
                hosts.add(ipObj);

            }
        }

        result.replace("hosts", hosts);
        if (clientInfo.type == ClientInfo.ClientType.JAVA
                && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
            result.put("dom", serviceName);
        } else {
            result.put("dom", NamingUtils.getServiceName(serviceName));
        }
        result.put("name", serviceName);
        result.put("cacheMillis", cacheMillis);
        result.put("lastRefTime", System.currentTimeMillis());
        result.put("checksum", service.getChecksum());
        result.put("useSpecifiedURL", false);
        result.put("clusters", clusters);
        result.put("env", env);
        result.replace("metadata", JacksonUtils.transferToJsonNode(service.getMetadata()));
        return result;
    }
}
