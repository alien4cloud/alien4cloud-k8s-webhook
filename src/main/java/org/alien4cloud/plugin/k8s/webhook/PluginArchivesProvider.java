package org.alien4cloud.plugin.k8s.webhook;

import alien4cloud.plugin.archives.AbstractArchiveProviderPlugin;
import org.springframework.stereotype.Component;

@Component("k8swebhook-archives-provider")
public class PluginArchivesProvider extends AbstractArchiveProviderPlugin {
    @Override
    protected String[] getArchivesPaths() {
        return new String[] { "csar" };
    }
}
