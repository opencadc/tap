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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.security.Principal;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Create table. This action creates a new database table and adds a description
 * to the tap_schema.
 * 
 * @author pdowler
 */
public class PutAction extends TablesAction {
    private static final Logger log = Logger.getLogger(PutAction.class);
    
    public PutAction() { 
    }

    @Override
    public void doAction() throws Exception {
        String[] target = getTarget();
        String schemaName = target[0];
        String tableName = target[1];
        
        log.debug("PUT: schema=" + schemaName + " table=" + tableName);
        
        checkWritable();
        
        if (schemaName == null && tableName == null) {
            throw new IllegalArgumentException("missing schema|table name in path");
        }
        
        TapSchemaDAO ts = getTapSchemaDAO();
        if (tableName != null) {
            TablesAction.checkSchemaWritePermissions(ts, schemaName, logInfo);
            createTable(ts, schemaName, tableName);
        } else if (schemaName != null) {
            checkIsAdmin();
            createSchema(ts, schemaName);
        }
        
        syncOutput.setCode(200); // should be 201
    }
    
    private void createSchema(TapSchemaDAO ts, String schema) throws Exception {
        log.debug("createSchema: " + schema + " START");
        SchemaDesc cur = ts.getSchema(schema, 0);
        if (cur != null) {
            throw new ResourceAlreadyExistsException("schema: " + schema);
        }

        SchemaDesc inputSchema = getInputSchema(schema);
        String owner = syncInput.getHeader("x-schema-owner"); // HACK
        if (inputSchema == null || owner == null) {
            throw new IllegalArgumentException("no input schema & owner");
        }
        
        IdentityManager im = AuthenticationUtil.getIdentityManager();
        Subject s;
        if (owner.startsWith("openid ")) {
            String oid = owner.replace("openid ", "");
            s = im.toSubject(oid);
        } else {
            // this will only work if the IdentityManager.augment call below
            // can map HttpPrincipal to the desired owner type
            s = new Subject();
            HttpPrincipal op = new HttpPrincipal(owner);
            s.getPrincipals().add(op);
        }
        
        // flag schema as created using the TAP API
        inputSchema.apiCreated = true;
        TapPermissions perms = new TapPermissions();
        perms.owner = im.augment(s);
        
        String[] createSQL = new String[] {
            "CREATE SCHEMA " + schema,
            "grant usage on schema " + schema + " to public",
            "grant select on all tables in schema " + schema + " to public"
        };

        DataSource ds = getDataSource();
        ts.setDataSource(ds);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            tm.startTransaction();
            
            if (getCreateSchemaEnabled()) {
                JdbcTemplate jdbc = new JdbcTemplate(ds);
                for (String sql : createSQL) {
                    log.debug(sql);
                    jdbc.execute(sql);
                }
            }
            log.debug("update tap_schema: " + inputSchema);
            ts.put(inputSchema);
            log.debug("set permissions: " + perms);
            ts.setSchemaPermissions(schema, perms);
            
            tm.commitTransaction();
        } catch (UnsupportedOperationException ex) {
            try {
                log.debug("PUT failed - rollback", ex);
                tm.rollbackTransaction();
                log.debug("PUT failed - rollback: OK");
            } catch (Exception oops) {
                log.error("PUT failed - rollback : FAIL", oops);
            }
            log.debug("createSchema: " + schema + " FAIL");
            throw ex;
        } catch (Exception ex) {
            try {
                log.error("PUT failed - rollback", ex);
                tm.rollbackTransaction();
                log.error("PUT failed - rollback: OK");
            } catch (Exception oops) {
                log.error("PUT failed - rollback : FAIL", oops);
            }
            log.debug("createSchema: " + schema + " FAIL");
            
            throw new RuntimeException("failed to create schema " + schema, ex);
        } finally {
            
            if (tm.isOpen()) {
                log.error("BUG: open transaction in finally - trying to rollback");
                try {
                    tm.rollbackTransaction();
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                log.debug("createSchema: " + schema + " FAIL");
                throw new RuntimeException("BUG: open transaction in finally");
            }
            log.debug("createSchema: " + schema + " DONE");
        }
        
    }

    private void createTable(TapSchemaDAO ts, String schemaName, String tableName) throws Exception {
        TableDesc inputTable = getInputTable(schemaName, tableName);
        if (inputTable == null) {
            throw new IllegalArgumentException("no input table");
        }
        
        // strip out some incoming table metadata
        // - indexed flag (separate index creation)
        for (ColumnDesc cd : inputTable.getColumnDescs()) {
            cd.indexed = false;
        }
        DataSource ds = getDataSource();
        ts.setDataSource(ds);
        TableDesc td = ts.getTable(tableName);
        if (td != null) {
            throw new ResourceAlreadyExistsException("table " + tableName + " already exists");
        }
        
        Profiler prof = new Profiler(PutAction.class);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            // create table
            TableCreator tc = new TableCreator(ds);
            tc.createTable(inputTable);
            prof.checkpoint("create-table");
            
            // add to tap_schema
            // flag table as created using the API to allow table deletion in the DeleteAction
            inputTable.apiCreated = true;
            ts.put(inputTable);
            prof.checkpoint("insert-into-tap-schema");
            
            // set the permissions to be initially private
            TapPermissions tablePermissions = new TapPermissions(
                AuthenticationUtil.getCurrentSubject(), false, null, null);
            ts.setTablePermissions(tableName, tablePermissions);
            prof.checkpoint("set-permissions");
            
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (Exception ex) {
            try {
                log.error("PUT failed - rollback", ex);
                tm.rollbackTransaction();
                prof.checkpoint("rollback-transaction");
                log.error("PUT failed - rollback: OK");
            } catch (Exception oops) {
                log.error("PUT failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to create/add " + tableName, ex);
        } finally { 
            if (tm.isOpen()) {
                log.error("BUG: open transaction in finally - trying to rollback");
                try {
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
        syncOutput.setCode(200);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new TablesInputHandler(INPUT_TAG);
    }
    
    private String getInputSchemaOwner() {
        Object in = syncInput.getContent(INPUT_TAG);
        if (in == null) {
            throw new IllegalArgumentException("no input: expected a document describing the schema owner for create");
        }
        if (in instanceof String) {
            String schemaOwner = (String) in;
            return schemaOwner;
        }
        throw new RuntimeException("BUG: no input schema owner");
    }
}
