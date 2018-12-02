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


import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.version.KeyValue;
import ca.nrc.cadc.db.version.KeyValueDAO;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Utility class with static methods for checking permissions.
 * 
 * @author pdowler
 */
class Util {
    private static final Logger log = Logger.getLogger(Util.class);

    private Util() { 
    }
    
    static String getSchemaFromTable(String tableName) {
        String[] st = tableName.split("[.]");
        if (st.length == 2) {
            return st[0];
        }
        throw new IllegalArgumentException("invalid table name: " + tableName + " (expected: <schema>.<table>)");
    }
    
    static void checkSchemaWritePermission(DataSource ds, String schemaName) {
        Subject owner = getOwner(ds, schemaName);
        if (owner == null) {
            // not listed : no one has permission
            throw new AccessControlException("permission denied");
        }
        
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : owner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return;
                }
            }
        }
        
        URI rwGroup = getReadWriteGroup(ds, schemaName);
        // TODO: see if cur is a member of rwGroup: LocalAuthority, GMSClient, blah blah
        
        throw new AccessControlException("permission denied");
    }
    
    static void checkTableWritePermission(DataSource ds, String tableName) throws ResourceNotFoundException {
        Subject owner = getOwner(ds, tableName);
        if (owner == null) {
            Subject schemaOwner = getOwner(ds, getSchemaFromTable(tableName));
            if (schemaOwner == null) {
                throw new AccessControlException("permission denied");
            } else {
                throw new ResourceNotFoundException("not found: " + tableName);
            }
        }
        
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : owner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return;
                }
            }
        }
        // not owner: group write permission check
        
        // - check group write on schema?
        String schemaName = Util.getSchemaFromTable(tableName);
        
        // - check group write on table?
        
        throw new AccessControlException("permission denied");
    }
    
    static void setTableOwner(DataSource ds, String tableName, Subject s) {
        IdentityManager im = AuthenticationUtil.getIdentityManager();
        if (im == null) {
            throw new RuntimeException("CONFIG: no IdentityManager implementation available");
        }
        KeyValue kv = new KeyValue(tableName + ".owner");
        
        KeyValueDAO dao = new KeyValueDAO(ds, null, "tap_schema");
        
        if (s == null) {
            dao.delete(kv.getName());
            log.debug("setOwner: " + kv.getName() + " deleted");
        } else {
            kv.value = im.toOwner(s).toString();
            dao.put(kv);
            log.debug("setOwner: " + kv.getName() + " = " + kv.value);
        }
    }
    
    // can be schemaName or tableName
    static Subject getOwner(DataSource ds, String name) {
        try {
            KeyValueDAO dao = new KeyValueDAO(ds, null, "tap_schema");
            String key = name + ".owner";
            KeyValue kv = dao.get(key);
            if (kv == null || kv.value == null) {
                return null;
            }

            IdentityManager im = AuthenticationUtil.getIdentityManager();
            if (im == null) {
                throw new RuntimeException("CONFIG: no IdentityManager implementation available");
            }
            Subject s = im.toSubject(kv.value);
            log.debug("schema: " + name + " owner: " + s);
            return s;
        } catch (RuntimeException rethrow) {
            throw rethrow;
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: failed to find owner for object " + name, ex);
        }
    }
    
    static URI getReadWriteGroup(DataSource ds, String name) {
        try {
            KeyValueDAO dao = new KeyValueDAO(ds, null, "tap_schema");
            String key = name + ".rw-group";
            KeyValue kv = dao.get(key);
            if (kv == null || kv.value == null) {
                return null;
            }

            URI ret = new URI(kv.value);
            log.debug("schema: " + name + " RW group: " + ret);
            return ret;
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: failed to find RW group for object " + name, ex);
        }
    }
    
    // TODO: what REST API is gojing to call this?
    // POST /youcat/blah/tables/{name}?rw={group URI} where {name} is a schema name or table name?
    static void setReadWriteGroup(DataSource ds, String name, URI group) {
        
        KeyValue kv = new KeyValue(name + ".rw-group");
        
        KeyValueDAO dao = new KeyValueDAO(ds, null, "tap_schema");
        
        if (group == null) {
            dao.delete(kv.getName());
            log.debug("setReadWriteGroup: " + kv.getName() + " deleted");
        } else {
            kv.value = group.toASCIIString();
            dao.put(kv);
            log.debug("setReadWriteGroup: " + kv.getName() + " = " + kv.value);
        }
    }
}
