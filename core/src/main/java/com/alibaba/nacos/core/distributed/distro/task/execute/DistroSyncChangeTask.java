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

package com.alibaba.nacos.core.distributed.distro.task.execute;

import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroFailedTaskHandler;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.utils.Loggers;

/**
 * Distro sync change task.
 *
 * @author xiweng.yy
 */
public class DistroSyncChangeTask extends AbstractDistroExecuteTask {

    private final DistroComponentHolder distroComponentHolder;

    public DistroSyncChangeTask(DistroKey distroKey, DistroComponentHolder distroComponentHolder) {
        super(distroKey);
        this.distroComponentHolder = distroComponentHolder;
    }

    //执行任务
    @Override
    public void run() {
        Loggers.DISTRO.info("[DISTRO-START] {}", toString());
        try {
            // 资源类型
            String type = getDistroKey().getResourceType();
            // 这个要去DistroHttpRegistry#doRegister 方法去看，因为是在这个方法里面向compont注册的
            DistroData distroData = distroComponentHolder.findDataStorage(type).getDistroData(getDistroKey());
            // type是change
            distroData.setType(DataOperation.CHANGE);
            // todo 获取通信组件，进行通信  syncData方法
            boolean result = distroComponentHolder.findTransportAgent(type).syncData(distroData, getDistroKey().getTargetServer());
            // 判断是否失败
            if (!result) {
                // 处理失败的task
                handleFailedTask();
            }
            Loggers.DISTRO.info("[DISTRO-END] {} result: {}", toString(), result);
        } catch (Exception e) {
            Loggers.DISTRO.warn("[DISTRO] Sync data change failed.", e);
            // 处理失败的task
            handleFailedTask();
        }
    }

    private void handleFailedTask() {
        String type = getDistroKey().getResourceType();
        DistroFailedTaskHandler failedTaskHandler = distroComponentHolder.findFailedTaskHandler(type);
        if (null == failedTaskHandler) {
            Loggers.DISTRO.warn("[DISTRO] Can't find failed task for type {}, so discarded", type);
            return;
        }
        failedTaskHandler.retry(getDistroKey(), DataOperation.CHANGE);
    }

    @Override
    public String toString() {
        return "DistroSyncChangeTask for " + getDistroKey().toString();
    }
}
