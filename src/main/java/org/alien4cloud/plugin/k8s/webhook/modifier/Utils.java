package org.alien4cloud.plugin.k8s.webhook.modifier;

import alien4cloud.utils.PropertyUtil;
import static alien4cloud.utils.AlienUtils.safe;

import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.tosca.model.CSARDependency;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.PolicyTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;

import static org.alien4cloud.plugin.kubernetes.csar.Version.K8S_CSAR_VERSION;
import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.NAMESPACE_RESOURCE_NAME;

import static org.alien4cloud.plugin.k8s.webhook.policies.PolicyModifier.PSEUDORESOURCE_POLICY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.Set;

public class Utils {

    public static String getK8SCsarVersion(Topology topology) {
        for (CSARDependency dep : topology.getDependencies()) {
            if (dep.getName().equals("org.alien4cloud.kubernetes.api")) {
                return dep.getVersion();
            }
        }
        return K8S_CSAR_VERSION;
    }

    public static String getNamespace(Topology topology, FlowExecutionContext context, ObjectMapper mapper) {
        NodeTemplate kubeNS = topology.getNodeTemplates().get((String)context.getExecutionCache().get(NAMESPACE_RESOURCE_NAME));
        if (kubeNS != null) {
           try {
              ObjectNode spec = (ObjectNode) mapper.readTree(PropertyUtil.getScalarValue(kubeNS.getProperties().get("resource_spec")));
              return spec.with("metadata").get("name").textValue();
           } catch(Exception e) {
              return null;
           }
        } else {
           return null;
        }
   }

   public static boolean containsPseudoResources (FlowExecutionContext context) {
        /* get all PseudoResource policies targets on initial topology */
        Topology topology = (Topology)context.getExecutionCache().get(FlowExecutionContext.INITIAL_TOPOLOGY);
        Set<String> pseudoResources = new HashSet<String>();
        Set<PolicyTemplate> policies = TopologyNavigationUtil.getPoliciesOfType(topology, PSEUDORESOURCE_POLICY, true);
        for (PolicyTemplate policy : policies) {
           /* get all target nodes on current policy */
           pseudoResources.addAll(safe(policy.getTargets()));
        }
        return (pseudoResources.size() > 0);
   }
}
