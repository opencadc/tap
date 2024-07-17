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
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.io.IOException;
import java.security.AccessControlException;

import java.security.Principal;
import java.util.Set;
import java.util.TreeSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;

/**
 *
 * @author pdowler
 */
public abstract class TablesAction extends RestAction {
    private static final Logger log = Logger.getLogger(TablesAction.class);

    public static String ADMIN_KEY = "-admin-principal";
    public static String CREATE_SCHEMA_KEY = "-create-schema-in-db";
    
    protected static final String PERMS_CONTENTTYPE = "text/plain";
    protected static final String OWNER_KEY = "owner";
    protected static final String PUBLIC_KEY = "public";
    protected static final String RGROUP_KEY = "r-group";
    protected static final String RWGROUP_KEY = "rw-group";
    
    protected String jndiAdminKey;
    protected String jndiCreateSchemaKey;
    
    public TablesAction() { 
        super();
    }

    @Override
    public void initAction() throws Exception {
        super.initAction();
        this.jndiAdminKey = appName + TablesAction.ADMIN_KEY;
        this.jndiCreateSchemaKey = appName + TablesAction.CREATE_SCHEMA_KEY;
    }

    protected final DataSource getDataSource() {
        PluginFactory pf = new PluginFactory();
        DataSourceProvider dsf = pf.getDataSourceProvider();
        return dsf.getDataSource(super.syncInput.getRequestPath());
    }
    
    // package access so InlineContentHandler could call it via ref to parent action
    void checkWritableImpl() throws TransientException {
        super.checkWritable();
    }
    
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    String getTableName() throws ResourceNotFoundException {
        String[] ss = getTarget();
        if (ss == null) {
            return null;
        }
        return ss[1];
    }

    // return {schema} and {table}
    String[] getTarget() throws ResourceNotFoundException {
        String path = syncInput.getPath();
        // TODO: move this empty str to null up to SyncInput?
        if (path == null || path.isEmpty()) {
            return null;
        }
        String[] st = path.split("[.]");
        if (st.length > 2) {
            throw new ResourceNotFoundException("not found: " + path + " (reason: invalid schema|table name -- too many dots)");
        }
        String[] ret = new String[2];
        ret[0] = st[0];
        if (st.length == 2) {
            ret[1] = path;
        }
        return ret;
    }
    
    /**
     * Create and configure a TapSchemaDAO instance. 
     * 
     * @return 
     */
    protected final TapSchemaDAO getTapSchemaDAO() {
        PluginFactory pf = new PluginFactory();
        TapSchemaDAO dao = pf.getTapSchemaDAO();
        DataSource ds = getDataSource();
        dao.setDataSource(ds);
        dao.setOrdered(true);
        return dao;
    }
    
    // schema owner can drop
    // table owner can drop
    // no group permissions used
    void checkDropTablePermission(TapSchemaDAO dao, String tableName)
            throws AccessControlException, ResourceNotFoundException {
        
        String schemaName = Util.getSchemaFromTable(tableName);
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        TapPermissions tablePermissions = dao.getTablePermissions(tableName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }
        if (tablePermissions == null) {
            throw new ResourceNotFoundException("table not found: " + tableName);
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("drop table allowed: schema owner");
            return;
        }
        if (Util.isOwner(tablePermissions)) {
            super.logInfo.setMessage("drop table allowed: table owner");
            return;
        }
        throw new AccessControlException("permission denied");
    }
    
    // schema owner can view schema permissions 
    TapPermissions checkViewSchemaPermissions(TapSchemaDAO dao, String schemaName)
            throws AccessControlException, ResourceNotFoundException {
        
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }
        if (schemaPermissions.owner == null) {
            super.logInfo.setMessage("view table allowed: null schema owner");
            return schemaPermissions;
        }
        if (schemaPermissions.isPublic) {
            super.logInfo.setMessage("view table allowed: public schema");
            return schemaPermissions;
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("view schema permissions allowed: schema owner");
            return schemaPermissions;
        }
        if (checkIsAdmin()) {
            super.logInfo.setMessage("view schema permissions allowed: admin");
            return schemaPermissions;
        }
        throw new AccessControlException("permission denied");
    }
    
    // schema owner can modify schema permissions 
    void checkModifySchemaPermissions(TapSchemaDAO dao, String schemaName)
            throws AccessControlException, ResourceNotFoundException {
        
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("modify schema permissions allowed: schema owner");
            return;
        }
        throw new AccessControlException("permission denied");
    }
    
    // schema owner and table owner can view table permissions 
    TapPermissions checkViewTablePermissions(TapSchemaDAO dao, String tableName)
            throws AccessControlException, ResourceNotFoundException {
        
        String schemaName = Util.getSchemaFromTable(tableName);
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }

        TapPermissions tablePermissions = dao.getTablePermissions(tableName);
        if (tablePermissions == null) {
            throw new ResourceNotFoundException("table not found: " + tableName);
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("view table permissions allowed: schema owner");
            return tablePermissions;
        }
        if (Util.isOwner(tablePermissions)) {
            super.logInfo.setMessage("view table permissions allowed: table owner");
            return tablePermissions;
        }
        if (checkIsAdmin()) {
            super.logInfo.setMessage("view table permissions allowed: admin");
            return tablePermissions;
        }
        throw new AccessControlException("permission denied");
    }
    
    // schema owner and table owner can modify table permissions 
    void checkModifyTablePermissionsPermissions(TapSchemaDAO dao, String tableName)
            throws AccessControlException, ResourceNotFoundException {
        
        String schemaName = Util.getSchemaFromTable(tableName);
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        TapPermissions tablePermissions = dao.getTablePermissions(tableName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }
        if (tablePermissions == null) {
            throw new ResourceNotFoundException("table not found: " + tableName);
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("modify table permissions allowed: schema owner");
            return;
        }
        if (Util.isOwner(tablePermissions)) {
            super.logInfo.setMessage("modify table permissions allowed: table owner");
            return;
        }
        throw new AccessControlException("permission denied");
    }
    
    // if anon or authenticated check public
    // if authenticated check schema and table owners, readGroups, readWriteGroups
    void checkTableReadPermissions(TapSchemaDAO dao, String tableName)
            throws AccessControlException, IOException, InterruptedException, ResourceNotFoundException {
        
        TapPermissions tablePermissions = dao.getTablePermissions(tableName);
        if (tablePermissions == null) {
            throw new ResourceNotFoundException("table not found: " + tableName);
        }
        
        String schemaName = Util.getSchemaFromTable(tableName);
        
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("schema not found: " + schemaName);
        }
        if (schemaPermissions.owner == null) {
            super.logInfo.setMessage("view table allowed: null schema owner");
            return;
        }
        if (schemaPermissions.isPublic) {
            super.logInfo.setMessage("view table allowed: public schema");
            return;
        }

        if (tablePermissions.owner == null) {
            super.logInfo.setMessage("view table allowed: null table owner");
            return;
        }
        if (tablePermissions.isPublic) {
            super.logInfo.setMessage("view table allowed: public table");
            return;
        }
        
        if (Util.isOwner(tablePermissions)) {
            super.logInfo.setMessage("view table allowed: table owner");
            return;
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("view table allowed: schema owner");
            return;
        }
        if (checkIsAdmin()) {
            super.logInfo.setMessage("view table allowed: admin");
            return;
        }
        
        // check group permissions
        // The serviceID should come from the read or readWrite group
        // in the future
        final IvoaGroupClient groupClient = new IvoaGroupClient();
        Set<GroupURI> readGroups = new TreeSet<>();
        if (schemaPermissions.readGroup != null) {
            readGroups.add(schemaPermissions.readGroup);
        }
        if (schemaPermissions.readWriteGroup != null) {
            readGroups.add(schemaPermissions.readWriteGroup);
        }
        if (tablePermissions.readGroup != null) {
            readGroups.add(tablePermissions.readGroup);
        }
        if (tablePermissions.readWriteGroup != null) {
            readGroups.add(tablePermissions.readWriteGroup);
        }
        
        GroupURI permittingGroup = Util.getPermittedGroup(groupClient, readGroups);
        if (permittingGroup != null) {
            super.logInfo.setMessage("view table allowed: member of group " + permittingGroup);
            return;
        }

        throw new AccessControlException("permission denied");
    }
    
    public void checkTableWritePermissions(TapSchemaDAO dao, String tableName)
            throws AccessControlException, IOException, ResourceNotFoundException {
        
        TablesAction.checkTableWritePermissions(dao, tableName, logInfo);
    }
    
    // if authenticated table owners, readWriteGroup members
    // static method here so that TableUpdateRunner can make this call
    static void checkTableWritePermissions(TapSchemaDAO dao, String tableName, WebServiceLogInfo logInfo)
            throws AccessControlException, IOException, ResourceNotFoundException {
        
        TapPermissions tablePermissions = dao.getTablePermissions(tableName); 
        if (tablePermissions == null) {
            throw new ResourceNotFoundException("table not found: " + tableName);
        }
        if (Util.isOwner(tablePermissions)) {
            logInfo.setMessage("table write allowed: table owner");
            return;
        }
        final IvoaGroupClient groupClient = new IvoaGroupClient();
        Set<GroupURI> permittedGroups = new TreeSet<>();
        if (tablePermissions.readWriteGroup != null) {
            permittedGroups.add(tablePermissions.readWriteGroup);
            GroupURI permittedGroup = Util.getPermittedGroup(groupClient, permittedGroups);
            if (permittedGroup != null) {
                logInfo.setMessage("schema write allowed: member of table group " + permittedGroup);
                return;
            }
        }
        throw new AccessControlException("permission denied");
    }
    
    // if authenticated check schema owner and readWriteGroup
    void checkSchemaWritePermissions(TapSchemaDAO dao, String schemaName) 
            throws AccessControlException, IOException, ResourceNotFoundException {
        
        TapPermissions schemaPermissions = dao.getSchemaPermissions(schemaName);
        if (schemaPermissions == null) {
            throw new ResourceNotFoundException("not found: " + schemaName);
        }
        if (Util.isOwner(schemaPermissions)) {
            super.logInfo.setMessage("schema write allowed: schema owner");
            return;
        }
        final IvoaGroupClient groupClient = new IvoaGroupClient();
        Set<GroupURI> permittedGroups = new TreeSet<>();
        if (schemaPermissions.readWriteGroup != null) {
            permittedGroups.add(schemaPermissions.readWriteGroup);
            GroupURI permittedGroup = Util.getPermittedGroup(groupClient, permittedGroups);
            if (permittedGroup != null) {
                super.logInfo.setMessage("schema write allowed: member of table group " + permittedGroup);
                return;
            }
        }
        throw new AccessControlException("permission denied");
    }
    
    // check if the caller is an admin
    boolean checkIsAdmin() {
        try {
            Context ctx = new InitialContext();
            HttpPrincipal admin = (HttpPrincipal) ctx.lookup(jndiAdminKey);
            if (admin != null) {
                Subject caller = AuthenticationUtil.getCurrentSubject();
                for (Principal p : caller.getPrincipals()) {
                    if (AuthenticationUtil.equals(admin, p)) {
                        return true;
                    }
                }
            }
        } catch (NamingException ex) {
            log.error("Failed to find JNDI key: " + jndiAdminKey, ex);
        }
        return false;
    }
    
    boolean getCreateSchemaEnabled() {
        try {
            Context ctx = new InitialContext();
            Boolean ret = (Boolean) ctx.lookup(jndiCreateSchemaKey);
            if (ret != null) {
                return ret;
            }
        } catch (NamingException ex) {
            log.error("Failed to find JNDI key: " + jndiCreateSchemaKey, ex);
        }
        return false;
    }
}
