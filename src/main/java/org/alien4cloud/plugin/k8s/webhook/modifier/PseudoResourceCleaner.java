package org.alien4cloud.plugin.k8s.webhook.modifier;

import alien4cloud.paas.wf.util.WorkflowUtils;
import alien4cloud.paas.wf.validation.WorkflowValidator;
import alien4cloud.tosca.context.ToscaContextual;
import static alien4cloud.utils.AlienUtils.safe;

import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.alm.deployment.configuration.flow.TopologyModifierSupport;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.PolicyTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.model.workflow.Workflow;
import org.alien4cloud.tosca.normative.constants.NormativeWorkflowNameConstants;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;

import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.A4C_KUBERNETES_ADAPTER_MODIFIER_TAG_REPLACEMENT_NODE_FOR;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_DEPLOYMENT_RESOURCE;

import static org.alien4cloud.plugin.k8s.webhook.policies.PolicyModifier.PSEUDORESOURCE_POLICY;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component("pseudoresource-cleaner")
public class PseudoResourceCleaner extends TopologyModifierSupport {
    @Override
    @ToscaContextual
    public void process(Topology topology, FlowExecutionContext context) {
        log.info("Processing topology " + topology.getId());

        try {
            WorkflowValidator.disableValidationThreadLocal.set(true);
            doProcess(topology, context);
        } catch (Exception e) {
            log.warn("Couldn't process PseudoResourceCleaner modifier, got " + e.getMessage());
        } finally {
            WorkflowValidator.disableValidationThreadLocal.remove();
        }
    }

    private void doProcess(Topology topology, FlowExecutionContext context) {
        /* get all k8s deployment resource nodes */
        Set<NodeTemplate> k8sNodes =  TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_DEPLOYMENT_RESOURCE, true);
        /* get all corresponding node names in initial topology */
        Map<String,String> nodeNames = new HashMap<String,String>();
        k8sNodes.forEach (deployNode -> {
           String nodeName = getNodeTagValueOrNull(deployNode, A4C_KUBERNETES_ADAPTER_MODIFIER_TAG_REPLACEMENT_NODE_FOR);
           nodeNames.put(nodeName, deployNode.getName());
        });

        Set<String> pseudoResources = new HashSet<String>();

        Topology init_topology = (Topology)context.getExecutionCache().get(FlowExecutionContext.INITIAL_TOPOLOGY);
        /* get all PseudoResource policies targets on initial topology */
        Set<PolicyTemplate> policies = TopologyNavigationUtil.getPoliciesOfType(init_topology, PSEUDORESOURCE_POLICY, true);
        for (PolicyTemplate policy : policies) {
           /* get all target nodes on current policy */
           Set<NodeTemplate> targetNodes = TopologyNavigationUtil.getTargetedMembers(init_topology, policy);
           /* get deployment nodes corresponding to target nodes if any */
           targetNodes.forEach (targetNode -> {
              String deployNodeName = nodeNames.get(targetNode.getName());
              if (deployNodeName != null) {
                 pseudoResources.add(deployNodeName);
              }
           });
        }
        log.debug ("nodes to be cleaned: {}", pseudoResources);
        if (pseudoResources.size() == 0) {
           return;
        }
        cleanupWorkflow (pseudoResources, topology, NormativeWorkflowNameConstants.INSTALL);
        cleanupWorkflow (pseudoResources, topology, NormativeWorkflowNameConstants.UNINSTALL);
    }

    /* cleanup one workflow for given pseudo resources */
    private void cleanupWorkflow (Set<String> nodeTemplates, Topology topology, String wf) {
        Workflow workflow = topology.getWorkflow(wf);
        if (workflow != null) {
            Set<String> stepsToRemove = new HashSet<String>();
            workflow.getSteps().forEach((s, workflowStep) -> {
                if (nodeTemplates.contains(workflowStep.getTarget())) {
                    stepsToRemove.add(s);
                }
            });
            for (String stepId : stepsToRemove) {
                WorkflowUtils.removeStep(workflow, stepId, true);
            }
        }
    }

}
