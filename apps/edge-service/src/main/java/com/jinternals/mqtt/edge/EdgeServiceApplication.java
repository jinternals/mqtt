package com.jinternals.mqtt.edge;

import com.jinternals.mqtt.edge.config.EdgeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The agent that runs on a site gateway. One instance per site; {@code app.edge.site-id} is the
 * only thing that differs between site 1 and site 2.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(EdgeProperties.class)
public class EdgeServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EdgeServiceApplication.class, args);
    }
}
