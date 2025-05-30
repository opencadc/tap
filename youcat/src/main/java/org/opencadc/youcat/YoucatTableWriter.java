package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.tap.DefaultTableWriter;
import ca.nrc.cadc.tap.PluginFactory;

import java.io.IOException;
import java.util.List;

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
            if (fieldIDs.containsAll(template.getIdentifiers())) {
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
