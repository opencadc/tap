/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
 */

package ca.nrc.cadc.vosi.actions;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;

/**
 * Set the permissions on a schema or table.
 * 
 * @author majorb
 *
 */
public class PostPermissionsAction extends TablesAction {
    
    private static final Logger log = Logger.getLogger(PostPermissionsAction.class);
    
    static final String TAP_PERMISSIONS_CONTENT = "tapPermissions";
    
    public PostPermissionsAction() {
    }
    
    @Override
    public void doAction() throws Exception {
        String[] target = getTarget();
        if (target == null) {
            throw new IllegalArgumentException("no schema|table name in path");
        }
        String name = target[0]; // schema
        if (target[1] != null) {
            name = target[1]; // table
        }
        log.debug("name: " + name);
        
        checkWritable();
        
        TapSchemaDAO dao = getTapSchemaDAO();
        
        // get the permissions first to ensure table exists and to ensure user
        // is allowed to view/modifiy permissions.
        if (Util.isSchemaName(name)) {
            TablesAction.checkModifySchemaPermissions(dao, name, logInfo);
            TapPermissions perms = dao.getSchemaPermissions(name);
            modifyPermissions(perms);
            log.debug("Setting schema permissions to: \n" + perms);
            dao.setSchemaPermissions(name, perms);
        } else if (Util.isTableName(name)) {
            TablesAction.checkModifyTablePermissions(dao, name, logInfo);
            TapPermissions perms = dao.getTablePermissions(name);
            modifyPermissions(perms);
            log.debug("Setting table permissions to: \n" + perms);
            dao.setTablePermissions(name, perms);
        } else {
            throw new IllegalArgumentException("No such object: " + name);
        }
        
        syncOutput.setCode(200);
    }
    
    private void modifyPermissions(TapPermissions orig) {
        Map<String, Object> perms = (Map<String, Object>) syncInput.getContent(TAP_PERMISSIONS_CONTENT);
        if (perms == null) {
            throw new IllegalArgumentException("No permission info");
        }
        
        log.debug("found perm keys: " + perms.keySet());
        for (String key : perms.keySet()) {
            if (key.equals(PUBLIC_KEY)) {
                orig.isPublic = (Boolean) perms.get(key);
                log.debug("changing " + PUBLIC_KEY + " to: " + orig.isPublic);
            }
            if (key.equals(RGROUP_KEY)) {
                orig.readGroup = (GroupURI) perms.get(key);
                log.debug("changing " + RGROUP_KEY + " to: " + orig.readGroup);
            }
            if (key.equals(RWGROUP_KEY)) {
                orig.readWriteGroup = (GroupURI) perms.get(key);
                log.debug("changing " + RWGROUP_KEY + " to: " + orig.readWriteGroup);
            }
        }
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new PermissionsInlineContentHandler();
    }
    
    public static class PermissionsInlineContentHandler implements InlineContentHandler {

        @Override
        public Content accept(String name, String contentType, InputStream inputStream)
                throws InlineContentException, IOException, ResourceNotFoundException {
            
            if (contentType == null || !contentType.equals(PERMS_CONTENTTYPE)) {
                throw new IllegalArgumentException("require content type of " + PERMS_CONTENTTYPE);
            }
            
            InputStreamReader isReader = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(isReader);
            String next = reader.readLine();
            
            Map<String, Object> params = new HashMap<>(4);
            
            while (next != null) {
                
                log.debug("Next inline param: " + next);
                String[] parts = next.split("[=]");
                boolean emptyValue = false;
                if (next.endsWith("=") && parts.length == 1) {
                    emptyValue = true;
                } else if (parts.length != 2) {
                    throw new IllegalArgumentException("not in expected format key=value: " + next);
                }
                
                if (parts[0].equalsIgnoreCase(OWNER_KEY)) {
                    if (!emptyValue) {
                        throw new IllegalArgumentException(OWNER_KEY + " cannot be set");
                    }
                } else if (parts[0].equalsIgnoreCase(PUBLIC_KEY)) {
                    if (emptyValue) {
                        throw new IllegalArgumentException("no value for " + PUBLIC_KEY);
                    }
                    if (parts[1].equalsIgnoreCase("true")) {
                        params.put(PUBLIC_KEY, Boolean.TRUE);
                    } else if (parts[1].equalsIgnoreCase("false")) {
                        params.put(PUBLIC_KEY, Boolean.FALSE);
                    } else {
                        throw new IllegalArgumentException("bad public value: " + parts[1]);
                    }
                } else if (parts[0].equalsIgnoreCase(RGROUP_KEY)) {
                    if (emptyValue) {
                        params.put(RGROUP_KEY, null);
                    } else {
                        params.put(RGROUP_KEY, new GroupURI(URI.create(parts[1])));
                    }
                } else if (parts[0].equalsIgnoreCase(RWGROUP_KEY)) {
                    if (emptyValue) {
                        params.put(RWGROUP_KEY, null);
                    } else {
                        params.put(RWGROUP_KEY, new GroupURI(URI.create(parts[1])));
                    }
                } else {
                    throw new IllegalArgumentException("unknown permissions key: " + parts[0]);
                }

                next = reader.readLine();
            }
            
            if (!params.isEmpty()) {
                Content content = new Content();
                content.name = TAP_PERMISSIONS_CONTENT;
                content.value = params;
                return content;
            }
            return null;
            
        }
        
    }

}


