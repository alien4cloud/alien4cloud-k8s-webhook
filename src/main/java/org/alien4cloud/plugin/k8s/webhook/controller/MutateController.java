package org.alien4cloud.plugin.k8s.webhook.controller;

import alien4cloud.common.MetaPropertiesService;
import alien4cloud.deployment.ArtifactProcessorService;
import alien4cloud.deployment.DeploymentService;
import alien4cloud.deployment.DeploymentRuntimeStateService;
import alien4cloud.exception.NotFoundException;
import alien4cloud.model.common.MetaPropertyTarget;
import alien4cloud.model.common.Tag;
import alien4cloud.model.deployment.DeploymentTopology;
import alien4cloud.model.orchestrators.locations.Location;
import alien4cloud.orchestrators.locations.services.LocationService;
import alien4cloud.tosca.context.ToscaContext;
import static alien4cloud.utils.AlienUtils.safe;
import alien4cloud.utils.CloneUtil;
import alien4cloud.utils.PropertyUtil;

import org.alien4cloud.alm.deployment.configuration.flow.TopologyModifierSupport;
import org.alien4cloud.tosca.model.definitions.AbstractPropertyValue;
import org.alien4cloud.tosca.model.definitions.ComplexPropertyValue;
import org.alien4cloud.tosca.model.definitions.DeploymentArtifact;
import org.alien4cloud.tosca.model.templates.Capability;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.PolicyTemplate;
import org.alien4cloud.tosca.model.templates.RelationshipTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.model.types.NodeType;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;

import static alien4cloud.paas.yorc.modifier.GangjaModifier.GANGJA_ARTIFACT_TYPE;
import static alien4cloud.plugin.k8s.spark.jobs.modifier.SparkJobsModifier.K8S_TYPES_SPARK_JOBS;
import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.A4C_KUBERNETES_ADAPTER_MODIFIER_TAG_REPLACEMENT_NODE_FOR;
import static org.alien4cloud.plugin.kubernetes.modifier.KubernetesAdapterModifier.K8S_TYPES_KUBECONTAINER;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.K8S_TYPES_DEPLOYMENT_RESOURCE;

import org.alien4cloud.plugin.k8s.webhook.modifier.WebhookConfiguration;
import org.alien4cloud.plugin.k8s.webhook.model.PV;
import static org.alien4cloud.plugin.k8s.webhook.policies.PolicyModifier.PSEUDORESOURCE_POLICY;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.admission.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.AdmissionReview;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


@Slf4j
@RestController
@RequestMapping({ "/rest/mutate", "/rest/v1/mutate", "/rest/latest/mutate" })
public class MutateController {

    @Inject
    private ArtifactProcessorService artifactProcessorService;
    @Inject
    private DeploymentRuntimeStateService deploymentRuntimeStateService;
    @Inject
    private DeploymentService deploymentService;
    @Inject
    private LocationService locationService;
    @Inject
    private MetaPropertiesService metaPropertiesService;

    @Inject
    private WebhookConfiguration conf;

    private final ObjectMapper mapper = new ObjectMapper();

    private static final String BAS_PROP = "Bac à sable";
    @ApiOperation(value = "Process AdmissionReview request from K8S")
    @RequestMapping(value = "/{envId}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public AdmissionReview mutate(@PathVariable String envId, @RequestBody AdmissionReview ar) {

        log.debug ("Request: name: {}, namespace: {}, object: {}", ar.getRequest().getName(),
                        ar.getRequest().getNamespace(), ar.getRequest().getObject() == null ? "<NONE>" : ar.getRequest().getObject().toString());

        AdmissionResponse resp = new AdmissionResponse();
        resp.setUid(ar.getRequest().getUid());
        ar.setResponse(resp);
        StringBuffer srcPatch = new StringBuffer("[");

        DeploymentTopology topology = null;
        try {
           topology = deploymentRuntimeStateService.getRuntimeTopologyFromEnvironment(envId);
           if (topology == null) {
              log.error ("Topology not found for env id {}", envId);
              return reject(ar, "Toplogy not found");
           } else if (!topology.isDeployed()) {
              log.error ("Topology not deployed for env id {}", envId);
              return reject(ar, "Toplogy not deployed");
           }
        } catch (NotFoundException e) {
           log.error ("Topology not found for env id {}", envId);
           return reject(ar, "Toplogy not found");
        }

        alien4cloud.model.deployment.Deployment deployment = null;
        try {
           deployment = deploymentService.getActiveDeploymentOrFail(envId);
        } catch (NotFoundException e) {
           log.error ("Active deployment not found for env id {}", envId);
           return reject(ar, "Active deployment not found");
        }
        Topology init_topology = deploymentRuntimeStateService.getUnprocessedTopology(deployment.getId());

        Location location = locationService.getOrFail(deployment.getLocationIds()[0]);
        String basMetaPropertyKey = this.metaPropertiesService.getMetapropertykeyByName(BAS_PROP, MetaPropertyTarget.LOCATION);
        String sBas = "false";
        if (basMetaPropertyKey == null) {
            log.warn("{} metaproperty does not exist", BAS_PROP);
        } else {
           sBas = safe(location.getMetaProperties()).get(basMetaPropertyKey);
           if (sBas == null) {
              log.info("{} metaproperty not set on location, using false", BAS_PROP);
              sBas = "false";
           } else {
              log.debug("{}:{}", BAS_PROP, sBas);
           }
        }
        boolean bas = Boolean.valueOf(sBas);
        String prio = conf.getPrioritesk8s().get("production");
        if (bas) {
           prio = conf.getPrioritesk8s().get("bacasable");
        }
        if (prio == null) {
           log.warn("Priority not set in configuration");
        }
        log.debug ("conf isRemoveResources: {}", conf.isRemoveResources());
        bas = bas && conf.isRemoveResources();

        if (ar.getRequest().getObject() instanceof Deployment) {
           log.info ("Request for a deployment {}", ar.getRequest().getName());
           Deployment dep = (Deployment)ar.getRequest().getObject();
           String nodeid = safe(dep.getMetadata().getLabels()).get("a4c_nodeid");
           if (nodeid == null) {
              nodeid = safe(dep.getSpec().getTemplate().getMetadata().getLabels()).get("a4c_nodeid");
           }
           if (nodeid == null) {
              log.error ("label a4c_nodeid not set");
              return reject(ar, "label a4c_nodeid must be set");
           }
           log.debug ("a4c_nodeid {}", nodeid);
           NodeTemplate initialNode = init_topology.getNodeTemplates().get(nodeid);
           if (initialNode == null) {
              log.error ("node not found");
              return reject(ar, "node not found");
           } else {
              log.debug ("Found a node for nodeid {}", nodeid);
              /* look for resource node replacing initial node */
              Set<NodeTemplate> deployNodes = getDeployNodes(topology);
              NodeTemplate k8snode = null;
              for (NodeTemplate deployNode : deployNodes) {
                 String nodeName = TopologyModifierSupport.getNodeTagValueOrNull(deployNode, A4C_KUBERNETES_ADAPTER_MODIFIER_TAG_REPLACEMENT_NODE_FOR);
                 if ((nodeName != null) && nodeName.equals(initialNode.getName())) {
                    k8snode = deployNode;
                    break;
                 }
              }
              if (k8snode == null) {
                 log.error ("Can not find k8s node for initial node {}",initialNode.getName());
                 return reject (ar, "K8S node not found");
              }
              log.debug ("Found k8s node for nodeid {}", nodeid);

              /* get resource spec */
              JsonNode spec = null;
              try {
                 spec = mapper.readTree(PropertyUtil.getScalarValue(k8snode.getProperties().get("resource_spec")));
              } catch(Exception e) {
                 log.error("Can't get node spec: {}", e.getMessage());
                 return reject (ar, "Can't read node spec property");
              }

              ToscaContext.init(init_topology.getDependencies());
              Set<NodeTemplate> containerNodes = TopologyNavigationUtil.getNodesOfType(init_topology, K8S_TYPES_KUBECONTAINER, true);
              for (NodeTemplate containerNode : containerNodes) {
                 log.debug ("Searching node {} for PV", containerNode.getName());
                 if (TopologyNavigationUtil.getImmediateHostTemplate (init_topology, containerNode) == initialNode) {
                    log.debug ("Container on current deployment...");

                    String qualifiedName = null;
                    List<Tag> tags = containerNode.getTags();
                    for (Tag tag: safe(tags)) {
                       if (tag.getName().equals("qualifiedName")) {
                          qualifiedName = tag.getValue();
                      }
                    }
                    if (qualifiedName == null) {
                       log.debug ("No qualified name");
                    }
                    else for (String pvNode : getOptPVService (init_topology, qualifiedName, containerNode))
                    {
                       if (!checkPV (qualifiedName, pvNode)) {
                          ToscaContext.destroy();
                          return reject (ar, "Not allowed to use PV");
                       }
                    }
                 }
              }
              ToscaContext.destroy();

              /* add a4c_nodeid to pods */
              boolean exist = (dep.getSpec().getTemplate().getMetadata().getLabels() != null);
              srcPatch = addToPatch (srcPatch, patchAdd ("/spec/template/metadata/labels", "a4c_nodeid", nodeid, exist));
                                     
              /* add labels from resource spec */
              JsonNode srcLabels = spec.with("spec").with("template").with("metadata").with("labels");
              StringBuffer labels = processPropsFromJson (srcLabels, "/spec/template/metadata/labels", true, "app");
              srcPatch = addToPatch (srcPatch, labels);

              /* add clusterPolicy label if required */
              Set<String> ghosts = getPseudoResourceNodes(init_topology);
              if (!ghosts.isEmpty() && !ghosts.contains(initialNode.getName()))  {
                 log.debug ("/spec/template/metadata/labels clusterPolicy: privileged");
                 srcPatch = addToPatch (srcPatch, patchAdd ("/spec/template/metadata/labels", "clusterPolicy", "privileged", true));
              }

              /* add annotations from resource spec */
              exist = (dep.getSpec().getTemplate().getMetadata().getAnnotations() != null);
              JsonNode srcAnnotations = spec.with("spec").with("template").with("metadata").with("annotations");
              StringBuffer annotations = processPropsFromJson (srcAnnotations, "/spec/template/metadata/annotations", exist, null);
              srcPatch= addToPatch (srcPatch, annotations);

              /* update env vars from resource spec */
              int nbcont = dep.getSpec().getTemplate().getSpec().getContainers().size();
              Iterator<JsonNode> jconts = spec.with("spec").with("template").with("spec").withArray("containers").elements();
              for (int icont = 0 ; icont < nbcont; icont++) {
                 List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(icont).getEnv();
                 JsonNode srcEnvs = jconts.next().withArray("env");
              
                 StringBuffer envmods = processEnvFromJson (srcEnvs, "/spec/template/spec/containers/" + icont + "/env", envs);
                 srcPatch = addToPatch (srcPatch, envmods);

                 /* remove resources if any */
                 if (bas) {
                    ResourceRequirements resources = dep.getSpec().getTemplate().getSpec().getContainers().get(icont).getResources();
                    if ((resources != null) && (resources.getLimits() != null)) {
                       srcPatch = addToPatch (srcPatch, new StringBuffer("{ \"op\": \"remove\", \"path\": \"/spec/template/spec/containers/" + icont + "/resources/limits\" }"));
                    }
                    if ((resources != null) && (resources.getRequests() != null)) {
                       srcPatch = addToPatch (srcPatch, new StringBuffer("{ \"op\": \"remove\", \"path\": \"/spec/template/spec/containers/" + icont + "/resources/requests\" }"));
                    }
                 }
              }

              /* set priority class */
              if (prio != null) {
                 srcPatch = addToPatch (srcPatch, new StringBuffer("{\"op\": \"add\", \"path\": \"/spec/template/spec/priorityClassName\", \"value\":\"" + prio + "\"}"));
              }
           }
        } else if (ar.getRequest().getObject() instanceof Pod) {
           String podName = ar.getRequest().getName();
           log.info ("Request for a pod {}", podName == null ? "<not named>" : podName);
           Pod pod = (Pod)ar.getRequest().getObject();
           String nodeid = safe(pod.getMetadata().getLabels()).get("a4c_nodeid");
           if (nodeid == null) {
              log.error ("label a4c_nodeid not set");
              return reject(ar, "label a4c_nodeid must be set");
           }
           log.debug ("a4c_nodeid {}", nodeid);
           NodeTemplate node = topology.getNodeTemplates().get(nodeid);
           if (node == null) {
              /* check initial topology (case of a container) */
              if (init_topology.getNodeTemplates().get(nodeid) == null) {
                 log.error ("node not found");
                 return reject(ar, "node not found");
              }
           } else {
              log.debug ("Found a node for nodeid {}", nodeid);
              /* process only pods created for Spark Jobs */
              if (isJobNode(topology, node)) {
                 /* update pod labels from node labels */
                 boolean exist = (pod.getMetadata().getLabels() != null);
                 StringBuffer labels = processPropsFromProps (node, "labels", "/metadata/labels", exist);
                 srcPatch.append(labels);
                 /* update pod annotations from node annotations */
                 exist = (pod.getMetadata().getAnnotations() != null);
                 StringBuffer annotations = processPropsFromProps (node, "annotations", "/metadata/annotations", exist);
                 srcPatch = addToPatch(srcPatch, annotations);

                 /* update pod env vars from node var_values */
                 int nbcont = pod.getSpec().getContainers().size();
                 for (int  icont = 0 ; icont < nbcont ; icont++) {
                    List<EnvVar> envs = pod.getSpec().getContainers().get(icont).getEnv();
                    StringBuffer envmods = processEnvFromVars (node, "var_values", "/spec/containers/" + icont + "/env", envs);
                    srcPatch = addToPatch (srcPatch, envmods);
                    /* remove resources */
                    if (bas) {
                       srcPatch = addToPatch (srcPatch, new StringBuffer("{ \"op\": \"remove\", \"path\": \"/spec/containers/" + icont + "/resources/limits\" }"));
                       srcPatch = addToPatch (srcPatch, new StringBuffer("{ \"op\": \"remove\", \"path\": \"/spec/containers/" + icont + "/resources/requests\" }"));
                    }
                 }

                 /* check gangja artefacts */
                 boolean existA = (pod.getMetadata().getAnnotations() != null) || (annotations.length() > 0);
                 boolean existL = (pod.getMetadata().getLabels() != null) || (labels.length() > 0);
                 for (Map.Entry<String, DeploymentArtifact> aa : node.getArtifacts().entrySet()) {
                    if (aa.getValue().getArtifactType().equals(GANGJA_ARTIFACT_TYPE)) {
                       DeploymentArtifact clonedArtifact = CloneUtil.clone(aa.getValue());
                       artifactProcessorService.processDeploymentArtifact(clonedArtifact, init_topology.getId());
                       log.debug ("File {}", clonedArtifact.getArtifactPath());
                       srcPatch = addToPatch(srcPatch, processFile(clonedArtifact.getArtifactPath(), "annotation", 
                                     "/metadata/annotations", existA,
                                     PropertyUtil.getPropertyValueFromPath(safe(node.getProperties()),"var_values")));
                       srcPatch = addToPatch(srcPatch, processFile(clonedArtifact.getArtifactPath(), "label", 
                                     "/metadata/labels", existL,
                                     PropertyUtil.getPropertyValueFromPath(safe(node.getProperties()),"var_values")));
                    }
                 }

                 /* set priority class */
                 if (prio != null) {
                    if (pod.getSpec().getPriority() != null) {
                       srcPatch = addToPatch (srcPatch, new StringBuffer("{\"op\": \"remove\", \"path\": \"/spec/priority\"}"));
                    }
                    srcPatch = addToPatch (srcPatch, new StringBuffer("{\"op\": \"add\", \"path\": \"/spec/priorityClassName\", \"value\":\"" + prio + "\"}"));
                 }
              }
           }
        }

        srcPatch.append("]");
        /* response will contain a base64 encoded array of JSONPatches */
        if (srcPatch.length() > 2) {
           log.debug ("Patch {}", srcPatch);
           resp.setPatch(Base64.getEncoder().encodeToString(srcPatch.toString().getBytes()));
           resp.setPatchType("JSONPatch");
        }

        resp.setAllowed(Boolean.TRUE);
        return ar;
    }

    /* add JSON Patch "added" if any to current patch "src" if any */
    private StringBuffer addToPatch (StringBuffer src, StringBuffer added) {
        if (added.length() > 0) {
           if (src.length() > 1) {
              src.append(",");
           }
           src.append(added);
        }
        return src;
    }

    /*
     * generate negative admission response with given message
     */
    private AdmissionReview reject (AdmissionReview ar, String msg) {
       AdmissionResponse resp = ar.getResponse();
       resp.setAllowed(Boolean.FALSE);
       resp.setStatus (new StatusBuilder().withCode(403).withMessage(msg).build());
       log.info ("Rejecting the request: {}", msg);
       return ar;
    }

    /*
     * check whether given node is a spark job node
     */
    private boolean isJobNode (Topology topology, NodeTemplate node) {
        boolean result = false;
        try {
            ToscaContext.init(topology.getDependencies());
            NodeType type = ToscaContext.getOrFail(NodeType.class, node.getType());
            if (type.getDerivedFrom().contains(K8S_TYPES_SPARK_JOBS)) {
               result = true;
            }
        } finally {
            ToscaContext.destroy();
        }
        return result;
    }

    /*
     * get deployment resource nodes from topology
     */
    private Set<NodeTemplate> getDeployNodes (Topology topology) {
        Set<NodeTemplate> result = null;
        try {
            ToscaContext.init(topology.getDependencies());
            result = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_DEPLOYMENT_RESOURCE, true);
        } finally {
            ToscaContext.destroy();
        }
        return result;
    }

    /*
     * generate JSON patches for given set of properties
     * add all values from node to k8s resource
     *  prop  : property in node
     *  path  : JSON base path in k8s resource
     *  exist : if true, k8s resource already contains values under base path
     */
    private StringBuffer processPropsFromProps (NodeTemplate node, String prop, String path, boolean exist) {
        StringBuffer result = new StringBuffer();
        AbstractPropertyValue vals = PropertyUtil.getPropertyValueFromPath(safe(node.getProperties()),prop);
        if ((vals != null) && vals instanceof ComplexPropertyValue) {
           ComplexPropertyValue valsPV = (ComplexPropertyValue)vals;
           Map<String,Object> valsMap = safe(valsPV.getValue());
           for (String val : valsMap.keySet()) {
              result = addToPatch (result, patchAdd (path, val, (String)valsMap.get(val), exist));
              if (!exist) {
                  exist = true;
              }
           }
        }
        return result;
    }

    /*
     * generate an "add" JSONPatch for a set of properties with optional existing values
     *  path  : JSON base path
     *  name  : new property name
     *  value : new property value
     *  exist : true if base path already contains properties
     */
    private StringBuffer patchAdd (String path, String name, String value, boolean exist) {
       value = escapeValue(value);
       name = escapePath(name);
       if (!exist) {
          return new StringBuffer("{\"op\": \"add\", \"path\": \"" + path + "\", \"value\": {\"" + name + "\": \"" + value + "\"}}");
       } else {
          return new StringBuffer("{\"op\": \"add\", \"path\": \"" + path + "/" + name + "\", \"value\": \"" + value + "\"}");
       }
    }


    /*
     * generate JSON patches to replace given set of env vars values from vars in node
     * replace values for existing vars in k8s resource defined in node vars
     *  prop : vars property in node
     *  path : JSON base path in k8s resource
     *  envs : env vars in k8s resource
     */
    private StringBuffer processEnvFromVars (NodeTemplate node, String prop, String path, List<EnvVar> envs) {
        StringBuffer result = new StringBuffer();
        AbstractPropertyValue vals = PropertyUtil.getPropertyValueFromPath(safe(node.getProperties()),prop);
        if ((vals != null) && vals instanceof ComplexPropertyValue) {
           ComplexPropertyValue valsPV = (ComplexPropertyValue)vals;
           Map<String,Object> valsMap = safe(valsPV.getValue());
           for (int idx = 0; idx < safe(envs).size(); idx++) {
              Object val = valsMap.get(envs.get(idx).getName());
              if (val != null) {
                 log.debug ("env[{}] {} => {} ({})", idx, envs.get(idx).getName(), val, val.getClass());
                 String sval = null;
                 if (val instanceof String) {
                    sval = (String)val;
                 } else if (val instanceof Map) {
                    sval = (String)((Map)val).get("value");
                 }
                 result = addToPatch (result, new StringBuffer("{\"op\": \"replace\", \"path\": \"" + path + "/" + idx + "/value\", \"value\": \"" + escapeValue(sval) + "\"}"));
              } else {
                 log.debug ("Can not find env var {} in node", envs.get(idx).getName());
              }
           }
        }
        return result;
    }

    private ThreadLocal<Pattern> jinja2varDetectionPattern = new ThreadLocal<Pattern>() {
        @Override
        protected Pattern initialValue() {
            return Pattern.compile("\\{\\{.*\\W?_\\.(\\w+)\\W?.*\\}\\}");
        }
    };

    /*
     * generate JSON patches to add properties found in Gangja file with variables replacement
     *  path  : Gangja file
     *  prop  : property in Gangja file
     *  path  : JSON base path in k8s resource
     *  exist : if true, k8s resource already contains values under base path
     *  vals  : variables property in node
     */
    private StringBuffer processFile(String path, String prop, String jsonpath, boolean exist, AbstractPropertyValue vals) {
        StringBuffer result = new StringBuffer();

        Map<String,Object> valsMap = null;

        if ((vals != null) && vals instanceof ComplexPropertyValue) {
           ComplexPropertyValue valsPV = (ComplexPropertyValue)vals;
           valsMap = safe(valsPV.getValue());
        } else {
           return result; 
        }

        try {
            BufferedReader r = new BufferedReader(new FileReader(path));

            // For each line of input, try matching in it.
            String line;
            while ((line = r.readLine()) != null) {
                log.debug ("Line {}", line);
                if (line.startsWith("--conf spark.kubernetes.driver." + prop + ".")) {
                   int sep = line.indexOf("=");
                   if (sep == -1) {
                      log.warn("Malformed line {} in file {}", line, path);
                      continue;
                   }
                   String name = line.substring(("--conf spark.kubernetes.driver." + prop + ".").length(), sep);
                   String value = line.substring(sep+1);
                   log.debug ("{} {} : {}", prop, name, value);

                   Matcher m = jinja2varDetectionPattern.get().matcher(value);
                   while (m.find()) {
                       String namevar = m.group(1);
                       Object val = valsMap.get(namevar);
                       if (val != null) {
                          log.debug ("var {} : {}", namevar, val);
                          String sval = null;
                          if (val instanceof String) {
                             sval = (String)val;
                          } else if (val instanceof Map) {
                             sval = (String)((Map)val).get("value");
                          }
                          value = value.replace("{{ _." + namevar + " }}", sval);
                          m = jinja2varDetectionPattern.get().matcher(value);
                       }
                   }
                   log.debug ("After replace {} {} : {}", prop, name, value);

                   result = addToPatch (result, patchAdd (jsonpath, name, value, exist));
                   if (!exist) {
                     exist = true;
                   }
                }
            }
        } catch (IOException e) {
            log.warn("Not able to parse file at {}", path);
        }
        return result;
    }

    /*
     * generate JSON patches for given set of JSON values
     * add all values from JSON node to k8s resource, "exclude" excepted
     *  src     : JSON node containing properties to add
     *  path    : JSON base path in k8s resource
     *  exist   : if true, k8s resource already contains values under base path
     *  exclude : property to exclude
     */
    private StringBuffer processPropsFromJson (JsonNode src, String path, boolean exist, String exclude) {
        StringBuffer result = new StringBuffer();
        Iterator<String> iter = src.fieldNames();
        while (iter.hasNext()) {
           String name = iter.next();
           if (name.equals(exclude)) {
              continue;
           }
           log.debug ("{} {}: {}", path, name, src.get(name).textValue());
           result = addToPatch (result, patchAdd (path, name, src.get(name).textValue(), exist));
           if (!exist) {
              exist = true;
           }
        }
        return result;
    }


    /*
     * generate JSON patches to replace given set of env vars values from vars in JSON
     * replace values for existing vars in k8s resource defined in JSON
     *  src  : JSON node containing env vars from node
     *  path : JSON base path in k8s resource
     *  envs : env vars in k8s resource
     */
    private StringBuffer processEnvFromJson (JsonNode src, String path, List<EnvVar> envs) {
        StringBuffer result = new StringBuffer();
        for (int idx = 0; idx < safe(envs).size(); idx++) {
           String name = envs.get(idx).getName();
           String value = getEnvValFromJson(src, name);
           if (value != null) {
              log.debug ("env[{}] {} => {}", idx, name, value);
              if (!value.startsWith ("${SERVICE_IP_LOOKUP")) {
                 result = addToPatch (result, new StringBuffer("{\"op\": \"replace\", \"path\": \"" + path + "/" + idx + "/value\", \"value\": \"" + escapeValue(value) + "\"}"));
              } else {
                 log.debug ("Skipping env[{}]", idx);
              }
           } else {
              log.debug ("Can not find env var {} in node", name);
           }
        }
        return result;
    }

    /*
     * look for an env var in JSON node
     *  src  : JSON node containing env vars from node
     *  name : name of env var to look for
     */
    private String getEnvValFromJson (JsonNode src, String name) {
       Iterator<JsonNode> iter = src.elements();
       while (iter.hasNext()) {
          JsonNode node = iter.next();
          if (node.get("name").textValue().equals(name)) {
             return node.get("value").textValue();
          }
       }
       return null;
    }

    /*
     * escapes a value for k8s
     */
    private String escapeValue (String value) {
       return value.replaceAll("\\\\", "\\\\\\\\");
    }

    /*
     * escapes a value for a path in a JSONPatch
     */
    private String escapePath (String name) {
       return name.replaceAll("/", "~1");
    }

    /* 
     * calls API to check whether use of PV in topoloy is allowed
     */
    private boolean checkPV(String module, String pv) {
       String url = conf.getCheckPVURL();
       if ((url == null) || (url.trim().equals(""))) {
          log.info ("No CheckPV URL configured");
          return true;
       }

       RestTemplate restTemplate = null;
       try {
          restTemplate = getRestTemplate();
       } catch (Exception e) {
          log.error ("Error creating restTemplate: {}", e.getMessage());
          return true;
       }

       String auth = conf.getCheckPVCredentials();
       HttpHeaders headers = new HttpHeaders();
       byte[] encodedAuth = Base64.getEncoder().encode( 
            auth.getBytes(Charset.forName("US-ASCII")) );
       headers.set("Authorization", "Basic " + new String(encodedAuth));       

       PV data = new PV();
       data.setQnamePV(pv);
       data.setQnameModule(module);
       try {
          log.debug ("Check PV request: {}", mapper.writeValueAsString(data));
       } catch (Exception e) {}
       try {
          HttpEntity<PV> request;
          request = new HttpEntity (data, headers);
          PV result = restTemplate.postForObject(url, request, PV.class);
          try {
             log.debug ("Check PV response: {}", mapper.writeValueAsString(result));
          } catch (Exception e) {}
          if (!result.getCode().startsWith("2")) {
             log.error ("Use of PV {} is not allowed for module {}", data.getQnamePV(), data.getQnameModule());
             return false;
          }
       } catch (HttpClientErrorException he) {
          log.warn ("HTTP error {}", he.getStatusCode());
       } catch (HttpServerErrorException he) {
          log.error ("HTTP error {}", he.getStatusCode());
       } catch (ResourceAccessException re) {
          log.error  ("Cannot send request: {}", re.getMessage());
       }
       
       return true;
    }

    /**
     * initialise rest without checking certificate
     **/
    private RestTemplate getRestTemplate() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                return true;
            }
        };
        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom().loadTrustMaterial(null,acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        RestTemplate restTemplate = new RestTemplate(requestFactory);
        return restTemplate;
    }

    /**
     * get PV services related to a container if any
     **/
    private Set<String> getOptPVService (Topology topology, String qualifiedName, NodeTemplate containerNode) {
       Set<String> result = new HashSet<String>();
       for (RelationshipTemplate rel : safe(containerNode.getRelationships()).values()) {
          log.debug ("Searching relation {}", rel.getType());
          if (rel.getType().equals(conf.getPvRelationshipType())) {
             NodeTemplate pv = topology.getNodeTemplates().get(rel.getTarget());
             log.debug ("Found node {}", pv.getName());
             Capability endpoint = safe(pv.getCapabilities()).get("pvk8s_endpoint");
             result.add(PropertyUtil.getScalarValue(safe(endpoint.getProperties()).get("qnamePV")));
         }
       }
       return result;
    }

    /**
     * get all nodes target of a PseudoResource policy
     */
    private Set<String> getPseudoResourceNodes(Topology topology) {
        Set<String> pseudoResources = new HashSet<String>();
        Set<PolicyTemplate> policies = TopologyNavigationUtil.getPoliciesOfType(topology, PSEUDORESOURCE_POLICY, true);
        for (PolicyTemplate policy : policies) {
           /* get all target nodes on current policy */
           pseudoResources.addAll(safe(policy.getTargets()));
        }
        return pseudoResources;
    }
}
