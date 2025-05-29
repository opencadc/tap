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

import org.apache.log4j.Logger;
import org.opencadc.datalink.ServiceDescriptorTemplate;

public class YoucatTableWriter extends DefaultTableWriter {

    private static final Logger log = Logger.getLogger(YoucatTableWriter.class);

    @Override
    protected void addMetaResources(VOTableDocument votableDocument, List<String> fieldIDs) throws IOException {
        RegistryClient regClient = new RegistryClient();

        // Use TemplateDAO to get descriptors
        TemplateDAO templateDAO = new TemplateDAO();
        List<ServiceDescriptorTemplate> templates = templateDAO.list(fieldIDs);
        Map<String, VOTableDocument> serviceDocs = new HashMap<>();

        for (ServiceDescriptorTemplate template : templates) {
            VOTableResource resource = template.getResource();

            if (resource != null && !template.getIdentifiers().isEmpty()) {
                for (String id : template.getIdentifiers()) {
                    if (fieldIDs.contains(id) && !serviceDocs.containsKey(id)) {
                        VOTableDocument voTableDocument = new VOTableDocument();
                        voTableDocument.getResources().add(resource);
                        serviceDocs.put(id, voTableDocument);
                        break;
                    }
                }
            }
        }

        for (String fid : fieldIDs) {
            VOTableDocument serviceDocument = serviceDocs.getOrDefault(fid, getDoc(fid));
            if (serviceDocument == null) {
                return; // TODO: verify - continue/return?
            }

            for (VOTableResource metaResource : serviceDocument.getResources()) {
                if ("meta".equals(metaResource.getType())) {
                    votableDocument.getResources().add(metaResource);
                    try {
                        URL accessURL = null;
                        URI resourceIdentifier = null;
                        URI standardID = null;
                        Iterator<VOTableParam> i = metaResource.getParams().iterator();
                        while (i.hasNext()) {
                            VOTableParam vp = i.next();
                            if (vp.getName().equals("accessURL")) {
                                accessURL = new URL(vp.getValue());
                            } else if (vp.getName().equals("resourceIdentifier")) {
                                resourceIdentifier = new URI(vp.getValue());
                            } else if (vp.getName().equals("standardID")) {
                                standardID = new URI(vp.getValue());
                            }
                        }
                        if (accessURL == null && resourceIdentifier != null && standardID != null) {
                            // try to augment resource with accessURL
                            Subject s = AuthenticationUtil.getCurrentSubject();
                            AuthMethod cur = AuthenticationUtil.getAuthMethod(s);
                            if (cur == null) {
                                cur = AuthMethod.ANON;
                            }
                            log.debug("resourceIdentifier=" + resourceIdentifier + ", standardID=" + standardID + ", authMethod=" + cur);
                            accessURL = regClient.getServiceURL(resourceIdentifier, standardID, cur);
                            if (accessURL != null) {
                                String surl = accessURL.toExternalForm();
                                String arraysize = Integer.toString(surl.length()); // fixed length since we know it
                                VOTableParam accessParam = new VOTableParam("accessURL", "char", arraysize, surl);
                                metaResource.getParams().add(accessParam);
                            } else {
                                // log the error but continue anyway
                                log.error("failed to find accessURL: resourceIdentifier=" + resourceIdentifier
                                        + ", standardID=" + standardID + ", authMethod=" + cur);
                            }
                        }
                    } catch (URISyntaxException e) {
                        throw new RuntimeException("resourceIdentifier for fieldID: " + fid + " is invalid", e);
                    }
                }
            }
        }
    }
}
