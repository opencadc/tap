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
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.version.KeyValue;
import ca.nrc.cadc.db.version.KeyValueDAO;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.Set;
import java.util.TreeSet;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public abstract class TablesAction extends RestAction {
    private static final Logger log = Logger.getLogger(TablesAction.class);

    private Class tapSchemaImpl = TapSchemaDAO.class;
    private String dataSourceName = "jdbc/tapadm";
    
    public TablesAction() { 
    }

    /**
     * Override the default use of TapSchemaDAO to use a subclass.
     * 
     * @param tapSchemaImpl 
     */
    public void setTapSchemaImpl(Class tapSchemaImpl) {
        if (tapSchemaImpl == null) {
            throw new IllegalArgumentException("setTapSchemaImpl: arg cannot be null");
        }
        this.tapSchemaImpl = tapSchemaImpl;
    }

    /**
     * Override the default data source name (jdbc/tapadm).
     * 
     * @param dataSourceName 
     */
    public void setDataSourceName(String dataSourceName) {
        if (dataSourceName == null) {
            throw new IllegalArgumentException("setDataSourcename: arg cannot be null");
        }
        this.dataSourceName = dataSourceName;
    }
    
    protected final DataSource getDataSource() {
        try {
            return DBUtil.findJNDIDataSource(dataSourceName);
        } catch (NamingException ex) {
            throw new RuntimeException("CONFIG: failed to find datasource " + dataSourceName, ex);
        } 
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    String getTableName() {
        log.debug("path: " + syncInput.getPath() 
                + "\ncomponent: " + syncInput.getComponentPath()
                + "\ncontext: " + syncInput.getContextPath()
                + "\nrequest: " + syncInput.getRequestPath());
        String path = syncInput.getPath();
        // TODO: move this empty str to null up to SyncInput?
        if (path != null && path.isEmpty()) {
            return null;
        }
        return path;
    }
    
    /**
     * Create and configure a TapSchemaDAO instance. 
     * 
     * @return 
     */
    protected TapSchemaDAO getDAO() {
        try {
            DataSource ds = getDataSource();
            TapSchemaDAO dao = (TapSchemaDAO) tapSchemaImpl.newInstance();
            dao.setDataSource(ds);
            dao.setOrdered(true);
            return dao;
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: failed to instantiate " + tapSchemaImpl.getName(), ex);
        }
    }
    
    final String getSchemaFromTable(String tableName) {
        String[] st = tableName.split("[.]");
        if (st.length == 2) {
            return st[0];
        }
        throw new IllegalArgumentException("invalid table name: " + tableName + " (expected: <schema>.<table>)");
    }
    
    void checkSchemaWritePermission(String schemaName) {
        Subject owner = getSchemaOwner(schemaName);
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : owner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return;
                }
            }
        }
        // TODO: group write permission check
        throw new AccessControlException("permission denied");
    }
    
    void checkTableWritePermission(String tableName) {
        String schemaName = getSchemaFromTable(tableName);
        Subject owner = getSchemaOwner(schemaName);
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : owner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return;
                }
            }
        }
        // TODO: group write permission check
        throw new AccessControlException("permission denied");
    }
    
    private Subject getSchemaOwner(String schemaName) {
        try {
            DataSource ds = getDataSource();
            KeyValueDAO dao = new KeyValueDAO(ds, null, "tap_schema");
            String key = schemaName + ".owner";
            KeyValue kv = dao.get(key);
            if (kv == null || kv.value == null) {
                // not listed : no one has permission
                throw new AccessControlException("permission denied");
            }

            IdentityManager im = AuthenticationUtil.getIdentityManager();
            if (im == null) {
                throw new RuntimeException("CONFIG: no IdentityManager implementation available");
            }
            Subject s = im.toSubject(kv.value);
            log.debug("schema: " + schemaName + " owner: " + s);
            return s;
        } catch (AccessControlException rethrow) {
            throw rethrow;
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: failed to find schema owner", ex);
        }
    }
    
    private URI getSchemaWriteGroup(String schemaName) {
        throw new UnsupportedOperationException("group permissions not implemented");
    }
}
