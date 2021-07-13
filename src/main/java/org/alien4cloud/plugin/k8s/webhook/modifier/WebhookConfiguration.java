package org.alien4cloud.plugin.k8s.webhook.modifier;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Getter
@Setter
@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "webhook")
public class WebhookConfiguration {

    private String caFile;
    private String a4cUrl;
    private boolean removeResources = true;
    private Map<String,String> prioritesk8s;

    private String checkPVURL;
    private String checkPVCredentials;
    private String pvRelationshipType = "artemis.pvk8s.pub.relationships.ConnectsToPVK8S";
}
