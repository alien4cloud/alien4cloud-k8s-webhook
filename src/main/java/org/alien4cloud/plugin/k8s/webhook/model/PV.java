package org.alien4cloud.plugin.k8s.webhook.model;

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Getter
@Setter

@JsonIgnoreProperties(ignoreUnknown = true)
public class PV {
   /* request fields */
   String qnamePV;
   String qnameModule;

   /* response fields */
   String code;
   String message;
}
