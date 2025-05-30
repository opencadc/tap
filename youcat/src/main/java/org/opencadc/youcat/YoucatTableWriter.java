package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.DefaultTableWriter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;

import ca.nrc.cadc.tap.PluginFactory;
import org.apache.log4j.Logger;
import org.opencadc.datalink.ServiceDescriptorTemplate;

public class YoucatTableWriter extends DefaultTableWriter {

    private static final Logger log = Logger.getLogger(YoucatTableWriter.class);

    @Override
    protected void addMetaResources(VOTableDocument votableDocument, List<String> fieldIDs) throws IOException {
        super.addMetaResources(votableDocument, fieldIDs);

        // Use TemplateDAO to get descriptors
        TemplateDAO templateDAO = new TemplateDAO(new PluginFactory().getTapSchemaDAO());
        List<ServiceDescriptorTemplate> templates = templateDAO.list(fieldIDs);

        for (ServiceDescriptorTemplate template : templates) {
            if(fieldIDs.containsAll(template.getIdentifiers())){
                VOTableResource resource = template.getResource();
                if (resource == null) {
                    log.warn("No resource found for template: " + template.getName());
                }
                populateAccessURLParam(resource);
                votableDocument.getResources().add(resource);
            }
        }
    }
}
