/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.resource.kubernetes.kubeclient.decorators;

import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.ConfDirFiles;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Packages {@code kubernetes.conf-dir} into an owned immutable ConfigMap and mounts it over the
 * distribution {@code config} directory on master and worker Pods.
 *
 * <p>Independent from {@link ConfigMapDecorator}: that step mounts individual keys through {@code
 * subPath}, while this step replaces the whole configuration directory when the user opts in.
 * {@link #build()} runs only on the submitter; workers only {@link #decorate(V1Pod)}. Projected
 * files use mode {@code 0644} through the volume {@code defaultMode}.
 */
public final class ConfDirDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationConfig parameters;

    public ConfDirDecorator(String applicationId, KubernetesApplicationConfig parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        if (!parameters.isConfDirEnabled()) {
            return pod;
        }
        V1PodSpec podSpec = pod.getSpec();
        String volumeName = KubernetesConstants.CONF_DIR_VOLUME;
        String configMapName = ConfDirFiles.configMapName(applicationId);
        String mountPath = parameters.confDirectory();
        podSpec.addVolumesItem(
                new V1Volume()
                        .name(volumeName)
                        .configMap(
                                new V1ConfigMapVolumeSource()
                                        .name(configMapName)
                                        .defaultMode(KubernetesConstants.CONF_DIR_FILE_MODE)));
        podSpec.getContainers()
                .get(0)
                .addVolumeMountsItem(
                        new V1VolumeMount().name(volumeName).mountPath(mountPath).readOnly(true));
        podSpec.getContainers()
                .get(0)
                .addEnvItem(
                        new V1EnvVar()
                                .name(KubernetesConstants.SEATUNNEL_CONF_DIR_ENV)
                                .value(mountPath));
        return pod;
    }

    @Override
    public List<KubernetesResource<?>> build() {
        // Only the submitter packages local files. The master reuses the owned ConfigMap name when
        // decorating workers and never recreates it.
        if (parameters.getConfDir() == null) {
            return Collections.emptyList();
        }
        Path confDir = Paths.get(parameters.getConfDir());
        // Master/worker re-assembly persists a marker for mounts but cannot read the submitter path.
        if (!Files.isDirectory(confDir)) {
            return Collections.emptyList();
        }
        Map<String, String> files;
        try {
            files = ConfDirFiles.load(confDir, parameters.getConfigMapMaxSize());
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Failed to read kubernetes.conf-dir " + confDir.toAbsolutePath(), failure);
        }
        Map<String, String> data = new LinkedHashMap<>(files);
        return Collections.singletonList(
                new KubernetesConfigMap(
                        new V1ConfigMap()
                                .apiVersion(KubernetesConstants.CORE_API_VERSION)
                                .kind(KubernetesConstants.CONFIG_MAP_KIND)
                                .metadata(
                                        KubernetesResourceIdentity.metadata(
                                                applicationId,
                                                ConfDirFiles.configMapName(applicationId),
                                                KubernetesConstants.CONFIGURATION_ROLE))
                                .immutable(true)
                                .data(data)));
    }
}
