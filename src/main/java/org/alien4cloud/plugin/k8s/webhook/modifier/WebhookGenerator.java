package org.alien4cloud.plugin.k8s.webhook.modifier;

import alien4cloud.paas.wf.validation.WorkflowValidator;
import alien4cloud.tosca.context.ToscaContextual;
import static alien4cloud.utils.AlienUtils.safe;
import alien4cloud.utils.FileUtil;
import alien4cloud.utils.PropertyUtil;

import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.alm.deployment.configuration.flow.TopologyModifierSupport;
import org.alien4cloud.tosca.model.definitions.AbstractPropertyValue;
import org.alien4cloud.tosca.model.definitions.ScalarPropertyValue;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.model.workflow.Workflow;
import org.alien4cloud.tosca.normative.constants.NormativeRelationshipConstants;
import org.alien4cloud.tosca.normative.constants.NormativeWorkflowNameConstants;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;

import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.K8S_TYPES_KUBE_CLUSTER;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_DEPLOYMENT_RESOURCE;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_SIMPLE_RESOURCE;
import static alien4cloud.plugin.k8s.spark.jobs.modifier.SparkJobsModifier.K8S_TYPES_SPARK_JOBS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component("webhook-generator")
public class WebhookGenerator extends TopologyModifierSupport {

    @Inject
    private WebhookConfiguration conf;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @ToscaContextual
    public void process(Topology topology, FlowExecutionContext context) {
        log.info("Processing topology " + topology.getId());

        try {
            WorkflowValidator.disableValidationThreadLocal.set(true);
            doProcess(topology, context);
        } catch (Exception e) {
            log.warn("Couldn't process WebhookGenerator modifier, got " + e.getMessage());
        } finally {
            WorkflowValidator.disableValidationThreadLocal.remove();
        }
    }

    private void doProcess(Topology topology, FlowExecutionContext context) {
        /* get namespace */
        String namespace = Utils.getNamespace (topology, context, mapper);
        if (StringUtils.isBlank(namespace)) {
           log.info ("No namespace, can not perform");
           return;
        }

        /* get kube config */
        String kube_config = (String) context.getExecutionCache().get(K8S_TYPES_KUBE_CLUSTER);

        if (!Utils.containsPseudoResources(context)) {
           log.info ("No pseudo  resources found");
           return;
        }

        /* get certif */
        String certif = null;
        try {
            certif = FileUtil.readTextFile(FileSystems.getDefault().getPath(conf.getCaFile()));
        } catch (Exception e) {
            log.error ("Cannot read {}, error: {}", conf.getCaFile(), e.getMessage());
            return;
        }

        /* generate webhook definition */
        String whconf = "apiVersion: admissionregistration.k8s.io/v1\n" +
                        "kind: MutatingWebhookConfiguration\n" +
                        "metadata:\n" +
                        "  name: wh-" + namespace + "\n" +
                        "  labels:\n" +
                        "    a4c_id: wh-" + namespace + "\n" +
                        "webhooks:\n" +
                        "  - name: wh-" + namespace + ".webhook.a4c\n" + 
                        "    clientConfig:\n" + 
                        "      url: " + conf.getA4cUrl() + "/" + context.getEnvironmentContext().get().getEnvironment().getId() + "\n" + 
                        "      caBundle: " + Base64.getEncoder().encodeToString(certif.getBytes()) + "\n" + 
                        "    rules:\n" + 
                        "      - operations: [\"CREATE\"]\n" +
                        "        apiGroups: [\"*\"]\n" +
                        "        apiVersions: [\"*\"]\n" +
                        "        resources: [\"deployments\", \"pods\"]\n" +
                        "        scope: \"Namespaced\"\n" +
                        "    admissionReviewVersions: [\"v1\", \"v1beta1\"]\n" +
                        "    sideEffects: None\n" +
                        "    namespaceSelector:\n" +
                        "        matchLabels:\n" +
                        "          ns-clef-namespace: " + namespace + "\n";

        /* create webhook configuration */
        NodeTemplate wh = addNodeTemplate(null, topology, "Webhook", K8S_TYPES_SIMPLE_RESOURCE, Utils.getK8SCsarVersion(topology));

        setNodePropertyPathValue(null,topology,wh,"resource_type", new ScalarPropertyValue("mutatingwebhookconfiguration"));
        setNodePropertyPathValue(null,topology,wh,"resource_id", new ScalarPropertyValue("wh-" + namespace));
        setNodePropertyPathValue(null,topology,wh,"resource_spec", new ScalarPropertyValue(whconf));

        setNodePropertyPathValue(null, topology, wh, "kube_config", new ScalarPropertyValue(kube_config));

        Set<NodeTemplate> deployNodes = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_DEPLOYMENT_RESOURCE, false);
        Set<NodeTemplate> jobNodes = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_SPARK_JOBS, true);
        for (NodeTemplate deployNode: deployNodes) {
            addRelationshipTemplate(null,topology,wh,deployNode.getName(),NormativeRelationshipConstants.DEPENDS_ON,"dependency", "feature");
        }
        for (NodeTemplate jobNode: jobNodes) {
            addRelationshipTemplate(null,topology,wh,jobNode.getName(),NormativeRelationshipConstants.DEPENDS_ON,"dependency", "feature");
        }
    }
}
