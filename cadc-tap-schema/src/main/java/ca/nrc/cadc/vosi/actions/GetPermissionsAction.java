/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

import java.io.OutputStream;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NumericPrincipal;
import ca.nrc.cadc.gms.GroupURI;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;

/**
 * Return the permissions for the object identified by the 'name'
 * parameter.
 * 
 * @author majorb
 *
 */
public class GetPermissionsAction extends TablesAction {

    @Override
    public void doAction() throws Exception {
        String name = syncInput.getParameter("name");
        if (name == null) {
            throw new IllegalArgumentException( "Missing param: name");
        }
        
        TapSchemaDAO dao = getTapSchemaDAO();
        TapPermissions permissions = null;
        if (Util.isSchemaName(name)) {
            permissions = checkViewSchemaPermissions(dao, name);
        } else if (Util.isTableName(name)) {
            permissions = checkViewTablePermissions(dao, name);
        } else {
            throw new IllegalArgumentException("No such object: " + name);
        }
        
        syncOutput.setCode(200);
        syncOutput.setHeader("Content-Type", PERMS_CONTENTTYPE);
        
        StringBuilder sb = new StringBuilder();
        String ownerString = getOwnerString(permissions.owner);
        String readGroupString = getGroupString(permissions.readGroup);
        String readWriteGroupString = getGroupString(permissions.readWriteGroup);
        sb.append(OWNER_KEY).append(ownerString).append("\n");
        sb.append(PUBLIC_KEY).append(Boolean.toString(permissions.isPublic)).append("\n");
        sb.append(RGROUP_KEY).append(readGroupString).append("\n");
        sb.append(RWGROUP_KEY).append(readWriteGroupString).append("\n");
        
        OutputStream out = syncOutput.getOutputStream();
        out.write(sb.toString().getBytes());
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    // return the owner name, preferring userid, then DN, then
    // numeric, then whatever is left
    private String getOwnerString(Subject s) {
        if (s == null || s.getPrincipals().size() == 0) {
            return "";
        }
        if (s.getPrincipals(HttpPrincipal.class).size() > 0) {
            return s.getPrincipals(HttpPrincipal.class).iterator().next().getName();
        }
        if (s.getPrincipals(X500Principal.class).size() > 0) {
            return s.getPrincipals(X500Principal.class).iterator().next().getName();
        }
        if (s.getPrincipals(NumericPrincipal.class).size() > 0) {
            return s.getPrincipals(NumericPrincipal.class).iterator().next().getName();
        }
        return s.getPrincipals().iterator().next().getName();
    }
            
    private String getGroupString(GroupURI group) {
        if (group == null) {
            return "";
        }
        return group.toString();
    }

}
