package com.jinternals.mqtt.cloud;

import com.jinternals.mqtt.cloud.config.CloudProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The central service. Issues commands to any site, ingests telemetry and health from all of them,
 * and exposes both over HTTP.
 *
 * <p>It connects only to the cloud broker. Sites are not addressable from here at the network
 * level — they are behind NAT on links that come and go — so every interaction with a site is a
 * publish to the cloud broker that a site's bridge collects when it can.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(CloudProperties.class)
public class CloudServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CloudServiceApplication.class, args);
    }
}
