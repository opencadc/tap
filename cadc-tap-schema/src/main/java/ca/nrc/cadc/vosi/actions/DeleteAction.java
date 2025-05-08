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

import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Drop table. This action drops a database table and removes the description
 * to the tap_schema.
 * 
 * @author pdowler
 */
public class DeleteAction extends TablesAction {
    private static final Logger log = Logger.getLogger(DeleteAction.class);

    public DeleteAction() { 
    }

    @Override
    public void doAction() throws Exception {
        String[] target = getTarget();
        String schemaName = target[0];
        String tableName = target[1];
        log.debug("DELETE: schema=" + schemaName + " table=" + tableName);
        
        checkWritable();
        
        if (tableName == null && schemaName == null) {
            throw new IllegalArgumentException("missing schema|table name in path");
        }
        
        TapSchemaDAO ts = getTapSchemaDAO();
        if (tableName != null) {
            TablesAction.checkDropTablePermission(ts, tableName, logInfo);
            dropTable(ts, tableName);
        } else {
            checkIsAdmin();
            dropSchema(ts, schemaName);
        }
            
        syncOutput.setCode(200);
    }
    
    private void dropSchema(TapSchemaDAO ts, String schemaName) throws ResourceNotFoundException {
        Profiler prof = new Profiler(DeleteAction.class);
        DataSource ds = getDataSource();
        ts.setDataSource(ds);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            // if the schema was created with the API and is empty: drop
            // otherwise only delete the table from the tap_schema
            SchemaDesc schemaDesc = ts.getSchema(schemaName, 0);
            if (schemaDesc == null) {
                throw new ResourceNotFoundException("not found: " + schemaName);
            }
            // TapSchemaDAO checks that schema is empty
            
            if (getCreateSchemaEnabled() && schemaDesc.apiCreated) {
                String sql = "DROP SCHEMA " + schemaName;
                JdbcTemplate jdbc = new JdbcTemplate(ds);
                log.debug(sql);
                jdbc.execute(sql);
                prof.checkpoint("delete-table");
            }
            
            // remove from tap_schema last to minimise locking
            ts.deleteSchema(schemaName);
            prof.checkpoint("delete-from-tap-schema");
            
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (ResourceNotFoundException | UnsupportedOperationException rethrow) { 
            if (tm != null && tm.isOpen()) {
                tm.rollbackTransaction();
            }
            throw rethrow;
        } catch (Exception ex) {
            try {
                log.error("DELETE failed - rollback", ex);
                tm.rollbackTransaction();
                prof.checkpoint("rollback-transaction");
                log.error("DELETE failed - rollback: OK");
            } catch (Exception oops) {
                log.error("DELETE failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to delete " + schemaName, ex);
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
            }
        }
    }

    private void dropTable(TapSchemaDAO ts, String tableName) throws ResourceNotFoundException {
        Profiler prof = new Profiler(DeleteAction.class);
        DataSource ds = getDataSource();
        ts.setDataSource(ds);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            // drop table
            TableCreator tc = new TableCreator(ds);
            // if the table was created with the API, drop the table,
            // otherwise only delete the table from the tap_schema
            TableDesc tableDesc = ts.getTable(tableName);
            if (tableDesc.apiCreated) {
                tc.dropTable(tableName);
                prof.checkpoint("delete-table");
            }
            
            // remove from tap_schema last to minimise locking
            ts.delete(tableName);
            prof.checkpoint("delete-from-tap-schema");
            
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (ResourceNotFoundException rethrow) { 
            if (tm != null && tm.isOpen()) {
                tm.rollbackTransaction();
            }
            throw rethrow;
        } catch (Exception ex) {
            try {
                log.error("DELETE failed - rollback", ex);
                tm.rollbackTransaction();
                prof.checkpoint("rollback-transaction");
                log.error("DELETE failed - rollback: OK");
            } catch (Exception oops) {
                log.error("DELETE failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to delete " + tableName, ex);
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
            }
        }
    }
}
