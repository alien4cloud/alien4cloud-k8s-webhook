package org.alien4cloud.plugin.k8s.webhook.modifier;

import alien4cloud.paas.wf.validation.WorkflowValidator;
import alien4cloud.tosca.context.ToscaContextual;
import alien4cloud.utils.PropertyUtil;

import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.alm.deployment.configuration.flow.TopologyModifierSupport;
import org.alien4cloud.tosca.model.definitions.ComplexPropertyValue;
import org.alien4cloud.tosca.model.definitions.ScalarPropertyValue;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.normative.constants.NormativeRelationshipConstants;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;

import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.K8S_TYPES_KUBE_CLUSTER;
import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.NAMESPACE_RESOURCE_NAME;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_DEPLOYMENT_RESOURCE;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_SIMPLE_RESOURCE;
import static alien4cloud.plugin.k8s.spark.jobs.modifier.SparkJobsModifier.K8S_TYPES_SPARK_JOBS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component("runner-accesscontrol-generator")
public class RunnerAccessControlGenerator extends TopologyModifierSupport {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @ToscaContextual
    public void process(Topology topology, FlowExecutionContext context) {
        log.info("Processing topology " + topology.getId());

        try {
            WorkflowValidator.disableValidationThreadLocal.set(true);
            doProcess(topology, context);
        } catch (Exception e) {
            log.warn("Couldn't process RunnerAccessControlGenerator modifier, got " + e.getMessage());
        } finally {
            WorkflowValidator.disableValidationThreadLocal.remove();
        }
    }

    private void doProcess(Topology topology, FlowExecutionContext context) {

        if (!Utils.containsPseudoResources(context)) {
           log.info ("No pseudo  resources found");
           return;
        }

        /* get namespace */
        String namespace = Utils.getNamespace(topology, context, mapper);
        String nsNodeName = (String) context.getExecutionCache().get(NAMESPACE_RESOURCE_NAME);
        String kube_config = (String) context.getExecutionCache().get(K8S_TYPES_KUBE_CLUSTER); 

        String envId = context.getEnvironmentContext().get().getEnvironment().getId();

        Set<NodeTemplate> deployNodes = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_DEPLOYMENT_RESOURCE, false);

        /* create access control resources */
        String role = "kind: Role\n" +
                      "apiVersion: rbac.authorization.k8s.io/v1\n" +
                      "metadata:\n" +
                      "  name: " + envId + "\n" +
                      "  labels:\n" +
                      "    a4c_id: " + envId + "\n" +
                      "rules:\n" +
                      "  - apiGroups: [\"\"]\n" +
                      "    resources: [\"pods\"]\n" +
                      "    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n" +
                      "  - apiGroups: ['policy']\n" +
                      "    resources: ['podsecuritypolicies']\n" +
                      "    verbs: ['use']\n" +
                      "    resourceNames: ['privileged']\n" +
                      "  - apiGroups: [\"apps\"]\n" +
                      "    resources: [\"deployments\"]\n" +
                      "    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n" +
                      "  - apiGroups: [\"\"] # \"\" indicates the core API group\n" +
                      "    resources: [\"services\"]\n" +
                      "    verbs: [\"get\", \"watch\", \"list\", \"create\", \"update\"]\n" +
                      "  - apiGroups: [\"\"] # \"\" indicates the core API group\n" +
                      "    resources: [\"configmaps\"]\n" +
                      "    verbs: [\"get\", \"watch\", \"list\", \"create\", \"update\"]\n";
        createResource (topology, "RunnerRole", envId, "role", role, namespace, kube_config, deployNodes, nsNodeName);

        String serviceaccount = "apiVersion: v1\n" +
                                "kind: ServiceAccount\n" +
                                "metadata:\n" +
                                "  name: " + envId + "\n" +
                                "  labels:\n" +
                                "    a4c_id: " + envId + "\n";
        createResource (topology, "RunnerSA", envId, "serviceaccount", serviceaccount, namespace, kube_config, deployNodes, nsNodeName);

        String rolebinding = "kind: RoleBinding\n" +
                             "apiVersion: rbac.authorization.k8s.io/v1\n" +
                             "metadata:\n" +
                             "  name: " + envId + "\n" +
                             "  labels:\n" +
                             "    a4c_id: " + envId + "\n" +
                             "roleRef:\n" +
                             "  apiGroup: rbac.authorization.k8s.io\n" +
                             "  kind: Role\n" +
                             "  name: " + envId + "\n" +
                             "subjects:\n" +
                             "- kind: ServiceAccount\n" +
                             "  name: " + envId;
        createResource (topology, "RunnerRB", envId, "rolebinding",rolebinding,  namespace, kube_config, deployNodes, nsNodeName);


        for (NodeTemplate deployNode: deployNodes) {
            /* set environment id env var */
            addEnvToDeploy (deployNode, topology, envId);
             /* associate service account to resources */
            addSAToDeploy (deployNode, topology, envId);
        }
        Set<NodeTemplate> jobNodes = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_SPARK_JOBS, true);
        for (NodeTemplate jobNode: jobNodes) {
            /* set environment id env var */
            addEnvToJob (jobNode, topology, envId);
        }
    }

    private void createResource (Topology topology, String node_name, String resource_name, String resource_type,
                                 String resource_spec, String namespace, String kube_config, Set<NodeTemplate> deployNodes,
                                 String nsNodeName) {
        NodeTemplate resource = addNodeTemplate(null, topology, node_name, K8S_TYPES_SIMPLE_RESOURCE, Utils.getK8SCsarVersion(topology));

        setNodePropertyPathValue(null,topology,resource,"resource_type", new ScalarPropertyValue(resource_type));
        setNodePropertyPathValue(null,topology,resource,"resource_id", new ScalarPropertyValue(resource_name));
        setNodePropertyPathValue(null,topology,resource,"resource_spec", new ScalarPropertyValue(resource_spec));
        setNodePropertyPathValue(null, topology, resource, "kube_config", new ScalarPropertyValue(kube_config));
        if (!StringUtils.isBlank(namespace)) {
           setNodePropertyPathValue(null,topology,resource,"namespace", new ScalarPropertyValue(namespace));
           addRelationshipTemplate(null,topology, resource, nsNodeName, NormativeRelationshipConstants.DEPENDS_ON, "dependency", "feature");
        }
        deployNodes.forEach (node -> {
           addRelationshipTemplate(null,topology, node, node_name, NormativeRelationshipConstants.DEPENDS_ON, "dependency", "feature");
        });
    }

    private void addEnvToDeploy (NodeTemplate node, Topology topology, String envId) {
       /* get resource spec */
       JsonNode spec = null;
       try {
          spec = mapper.readTree(PropertyUtil.getScalarValue(node.getProperties().get("resource_spec")));
       } catch(Exception e) {
          log.error("Can't get node {} spec: {}", node.getName(), e.getMessage());
          return;
       }
       /* get container */
       ObjectNode container = (ObjectNode)spec.with("spec").with("template").with("spec").withArray("containers").elements().next();
       /* create new env var object */
       ObjectNode newEnv = JsonNodeFactory.instance.objectNode();
       newEnv.put ("name", "ENVIRONMENT_ID");
       newEnv.put ("value", envId);
       /* add new env var to container */
       if (!container.has("env")) {
          container.putArray("env");
       }
       container.withArray("env").add(newEnv);
       /* replace container in spec */
       String specStr = null;
       try {
          specStr = mapper.writeValueAsString(spec);
       } catch(Exception e) {
          log.error("Can't rewrite node {} spec: {}", node.getName(), e.getMessage());
          return;
       }
       setNodePropertyPathValue(null, topology, node, "resource_spec", new ScalarPropertyValue(specStr));
    }

    private void addEnvToJob (NodeTemplate node, Topology topology, String envId) {
       ComplexPropertyValue envsPV = (ComplexPropertyValue)PropertyUtil.getPropertyValueFromPath(node.getProperties(), "environments");
       Map<String,Object> envs = new HashMap<String,Object>();
       if (envsPV != null) {
          envs = envsPV.getValue();
       }
       envs.put ("ENVIRONMENT_ID", new ScalarPropertyValue(envId));
       setNodePropertyPathValue(null, topology, node, "environments", new ComplexPropertyValue(envs));
    }

    private void addSAToDeploy (NodeTemplate node, Topology topology, String envId) {
       /* get resource spec */
       JsonNode spec = null;
       try {
          spec = mapper.readTree(PropertyUtil.getScalarValue(node.getProperties().get("resource_spec")));
       } catch(Exception e) {
          log.error("Can't get node {} spec: {}", node.getName(), e.getMessage());
          return;
       }
       ObjectNode podSpec = (ObjectNode) spec.with("spec").with("template").with("spec");

       podSpec.put("serviceAccountName", envId);
       podSpec.put("automountServiceAccountToken", true);

       String specStr = null;
       try {
          specStr = mapper.writeValueAsString(spec);
       } catch(Exception e) {
          log.error("Can't rewrite node {} spec: {}", node.getName(), e.getMessage());
          return;
       }
       setNodePropertyPathValue(null, topology, node, "resource_spec", new ScalarPropertyValue(specStr));
    }
}
