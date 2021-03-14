/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.transport.decorator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.discovery.shared.resolver.ClusterResolver;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpClientFactory;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.EurekaTransportConfig;
import com.netflix.discovery.shared.transport.TransportClientFactory;
import com.netflix.discovery.shared.transport.TransportException;
import com.netflix.discovery.shared.transport.TransportUtils;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.discovery.EurekaClientNames.METRIC_TRANSPORT_PREFIX;

/**
 * {@link RetryableEurekaHttpClient} retries failed requests on subsequent servers in the cluster.
 * It maintains also simple quarantine list, so operations are not retried again on servers
 * that are not reachable at the moment.
 * <h3>Quarantine</h3>
 * All the servers to which communication failed are put on the quarantine list. First successful execution
 * clears this list, which makes those server eligible for serving future requests.
 * The list is also cleared once all available servers are exhausted.
 * <h3>5xx</h3>
 * If 5xx status code is returned, {@link ServerStatusEvaluator} predicate evaluates if the retries should be
 * retried on another server, or the response with this status code returned to the client.
 *
 * @author Tomasz Bak
 * @author Li gang
 */
public class RetryableEurekaHttpClient extends EurekaHttpClientDecorator {

    private static final Logger logger = LoggerFactory.getLogger(RetryableEurekaHttpClient.class);

    public static final int DEFAULT_NUMBER_OF_RETRIES = 3;

    private final String name;
    private final EurekaTransportConfig transportConfig;
    private final ClusterResolver clusterResolver;
    private final TransportClientFactory clientFactory;
    private final ServerStatusEvaluator serverStatusEvaluator;
    private final int numberOfRetries;

    private final AtomicReference<EurekaHttpClient> delegate = new AtomicReference<>();

    private final Set<EurekaEndpoint> quarantineSet = new ConcurrentSkipListSet<>();

    public RetryableEurekaHttpClient(String name,
                                     EurekaTransportConfig transportConfig,
                                     ClusterResolver clusterResolver,
                                     TransportClientFactory clientFactory,
                                     ServerStatusEvaluator serverStatusEvaluator,
                                     int numberOfRetries) {
        this.name = name;
        this.transportConfig = transportConfig;
        this.clusterResolver = clusterResolver;
        this.clientFactory = clientFactory;
        this.serverStatusEvaluator = serverStatusEvaluator;
        this.numberOfRetries = numberOfRetries;
        Monitors.registerObject(name, this);
    }

    @Override
    public void shutdown() {
        TransportUtils.shutdown(delegate.get());
        if(Monitors.isObjectRegistered(name, this)) {
            Monitors.unregisterObject(name, this);
        }
    }

    @Override
    protected <R> EurekaHttpResponse<R> execute(RequestExecutor<R> requestExecutor) {
        //用来保存所有Eureka Server信息(8761、8762、8763、8764....)
        List<EurekaEndpoint> candidateHosts = null;
        int endpointIdx = 0;
        //numberOfRetries的值代码写死默认为3次
        for (int retry = 0; retry < numberOfRetries; retry++) {

            EurekaHttpClient currentHttpClient = delegate.get();
            EurekaEndpoint currentEndpoint = null;
            if (currentHttpClient == null) {
                if (candidateHosts == null) {
                    // 首次进入循环时，获取全量的Eureka Server信息(8761、8762、8763、8764....)
                    candidateHosts = getHostCandidates();
                    if (candidateHosts.isEmpty()) {
                        throw new TransportException("There is no known eureka server; cluster server list is empty");
                    }
                }
                if (endpointIdx >= candidateHosts.size()) {
                    throw new TransportException("Cannot execute request on any known server");
                }

                //通过endpointIdx自增，依次获取Eureka Server信息，然后发送
                //注册的Post请求.
                currentEndpoint = candidateHosts.get(endpointIdx++);
                currentHttpClient = clientFactory.newClient(currentEndpoint);
            }

            try {
                // 发送注册的Post请求动作
                EurekaHttpResponse<R> response = requestExecutor.execute(currentHttpClient);
                if (serverStatusEvaluator.accept(response.getStatusCode(), requestExecutor.getRequestType())) {
                    delegate.set(currentHttpClient);
                    if (retry > 0) {
                        logger.info("Request execution succeeded on retry #{}", retry);
                    }
                    // 如果成功，则跳出循环
                    return response;
                }
                // 如果失败，则根据endpointIdx依次获取下一个Eureka Server
                logger.warn("Request execution failure with status code {}; retrying on another server if available", response.getStatusCode());
            } catch (Exception e) {
                // 向注册中心(Eureka Server)发起注册的post出现异常时，打印日志
                logger.warn("Request execution failed with message: {}", e.getMessage());  // just log message as the underlying client should log the stacktrace
            }

            // Connection error or 5xx from the server that must be retried on another server
            delegate.compareAndSet(currentHttpClient, null);

            //如果此次注册动作失败，将当前的信息保存到quarantineSet中(一个Set集合)
            if (currentEndpoint != null) {
                quarantineSet.add(currentEndpoint);
            }
        }

        //如果都失败,则以异常形式抛出
        throw new TransportException("Retry limit reached; giving up on completing the request");
    }

    public static EurekaHttpClientFactory createFactory(final String name,
                                                        final EurekaTransportConfig transportConfig,
                                                        final ClusterResolver<EurekaEndpoint> clusterResolver,
                                                        final TransportClientFactory delegateFactory,
                                                        final ServerStatusEvaluator serverStatusEvaluator) {
        return new EurekaHttpClientFactory() {
            @Override
            public EurekaHttpClient newClient() {
                return new RetryableEurekaHttpClient(name, transportConfig, clusterResolver, delegateFactory,
                        serverStatusEvaluator, DEFAULT_NUMBER_OF_RETRIES);
            }

            @Override
            public void shutdown() {
                delegateFactory.shutdown();
            }
        };
    }

    private List<EurekaEndpoint> getHostCandidates() {
        /**
         * 获取所有defaultZone配置的注册中心信息(Eureka Server)，
         * 在本文例子中代表4个(8761、8762、8763、8764)Eureka Server
         */
        List<EurekaEndpoint> candidateHosts = clusterResolver.getClusterEndpoints();

        /**
         * quarantineSet这个Set集合中保存的是不可用的Eureka Server
         * 此处是拿不可用的Eureka Server与全量的Eureka Server取交集
         */
        quarantineSet.retainAll(candidateHosts);

        /**
         * 根据RetryableClientQuarantineRefreshPercentage参数计算阈值
         * 该阈值后续会和quarantineSet中保存的不可用的Eureka Server个数
         * 作比较，从而判断是否返回全量的Eureka Server还是过滤掉不可用的
         * Eureka Server。
         */
        // If enough hosts are bad, we have no choice but start over again
        int threshold = (int) (candidateHosts.size() * transportConfig.getRetryableClientQuarantineRefreshPercentage());
        //Prevent threshold is too large
        if (threshold > candidateHosts.size()) {
            threshold = candidateHosts.size();
        }
        if (quarantineSet.isEmpty()) {
            // no-op
            /**
             * 首次进入的时候，此时quarantineSet（不可用的Eureka Server）为空，直接返回全量的
             * Eureka Server列表
             */
        } else if (quarantineSet.size() >= threshold) {
            /**
             * 将不可用的Eureka Server与threshold值相比较，如果不可
             * 用的Eureka Server个数大于阈值，则将之前保存的Eureka
             * Server内容直接清空，并返回全量的Eureka Server列表。
             */
            logger.debug("Clearing quarantined list of size {}", quarantineSet.size());
            quarantineSet.clear();
        } else {
            /**
             * 通过quarantineSet集合保存不可用的Eureka Server来过滤
             * 全量的EurekaServer，从而获取此次Eureka Client要注册要
             * 注册的Eureka Server实例地址。
             */
            List<EurekaEndpoint> remainingHosts = new ArrayList<>(candidateHosts.size());
            for (EurekaEndpoint endpoint : candidateHosts) {
                if (!quarantineSet.contains(endpoint)) {
                    remainingHosts.add(endpoint);
                }
            }
            candidateHosts = remainingHosts;
        }

        return candidateHosts;
    }

    @Monitor(name = METRIC_TRANSPORT_PREFIX + "quarantineSize",
            description = "number of servers quarantined", type = DataSourceType.GAUGE)
    public long getQuarantineSetSize() {
        return quarantineSet.size();
    }
}
