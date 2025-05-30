/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.uws.Job;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

/**
 * Given a DataSource to a TAP_SCHEMA, returns a TapSchema object containing the
 * TAP_SCHEMA data. The fully qualified names of tables in the tap_schema can be
 * modified in a subclass as long as the change(s) are made before the get
 * method is called (*TableName variables).
 */
public class TapSchemaDAO extends AbstractDAO {
    
    private static final Logger log = Logger.getLogger(TapSchemaDAO.class);

    /**
     * Integer representation of the TAP version (TAP 1.1 value is 11). This value
     * is expected to be appended to the tap_schema table names so that two versions
     * of the tap_schema tables can co-exist.
     */
    protected static final int TAP_VERSION = 11;

    // standard tap_schema table names
    protected String schemasTableName = "tap_schema.schemas" + TAP_VERSION;
    protected String tablesTableName = "tap_schema.tables" + TAP_VERSION;
    protected String columnsTableName = "tap_schema.columns" + TAP_VERSION;
    protected String keysTableName = "tap_schema.keys" + TAP_VERSION;
    protected String keyColumnsTableName = "tap_schema.key_columns" + TAP_VERSION;

    private String[] tsSchemaCols = new String[] { 
        "description", "utype", "schema_index", "api_created", "schema_name" };
    protected String orderSchemaClause = " ORDER BY schema_name";

    private String[] tsTablesCols = new String[] { 
        "schema_name", "table_type", "description", "utype", "table_index", "api_created", "table_name" };
    protected String orderTablesClause = " ORDER BY schema_name,table_index,table_name";

    private String[] tsColumnsCols = new String[] { "description", "utype", "ucd", "unit", "datatype", "arraysize",
        "xtype", "principal", "indexed", "std", "column_id", "column_index", "table_name", "column_name" };
    protected String orderColumnsClause = " ORDER BY table_name,column_index,column_name";

    private String[] tsKeysCols = new String[] { "key_id", "from_table", "target_table", "description,utype" };
    protected String orderKeysClause = " ORDER BY key_id,from_table,target_table";

    private String[] tsKeyColumnsCols = new String[] { "key_id", "from_column", "target_column" };
    protected String orderKeyColumnsClause = " ORDER BY key_id, from_column, target_column";

    // access control columns are present in the tables schema, tables, and columns,
    // but are not exposed as tap schema columns
    protected static String ownerCol = "owner_id";
    protected static String readAnonCol = "read_anon";
    protected static String readOnlyCol = "read_only_group";
    protected static String readWriteCol = "read_write_group";
    private static String[] accessControlCols = new String[] { ownerCol, readAnonCol, readOnlyCol, readWriteCol };

    protected Job job;
    private boolean ordered;
    private boolean useIntegerForBoolean = true;

    // Indicates function return datatype matches argument datatype.
    public static final String ARGUMENT_DATATYPE = "ARGUMENT_DATATYPE";

    public static final int MIN_DEPTH = 0; // schema only
    public static final int TAB_DEPTH = 1; // +tables
    public static final int MAX_DEPTH = 2; // +columns, keys, etc

    /**
     * Construct a new TapSchemaDAO.
     */
    public TapSchemaDAO() {
    }
    
    public void setJob(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    /**
     * Get the complete TapSchema. Calls get(MAX_DEPTH).
     * 
     * @return complete TapSchema
     */
    public TapSchema get() {
        // depth MIN = schemas and tables
        // depth MAX = columns + keys + functions
        return get(MAX_DEPTH);
    }

    /**
     * Creates and returns a TapSchema object representing some or all of the
     * content in TAP_SCHEMA. This method filters output based on what the current
     * Subject (user) is allowed to see.
     * 
     * @param depth in [0,2] where 0 is schema only, 1 is schema(s)+table(s), 2 is everything
     * @return TapSchema containing some or all of the content
     */
    public TapSchema get(int depth) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        log.debug("IdentityManager: " + identityManager);
        TapPermissionsMapper tapPermissionsMapper = new TapPermissionsMapper(identityManager);
        
        // TAP_SCHEMA.schemas
        GetSchemasStatement gss = new GetSchemasStatement(schemasTableName);
        if (ordered) {
            gss.setOrderBy(orderSchemaClause);
        }
        List<SchemaDesc> schemaDescs = jdbc.query(gss, new SchemaMapper(tapPermissionsMapper));

        if (depth > MIN_DEPTH) {
            // TAP_SCHEMA.tables
            GetTablesStatement gts = new GetTablesStatement(tablesTableName);
            if (ordered) {
                gts.setOrderBy(orderTablesClause);
            }
            List<TableDesc> tableDescs = jdbc.query(gts, new TableMapper(tapPermissionsMapper));
            addTablesToSchemas(schemaDescs, tableDescs);

            if (depth > TAB_DEPTH) {
                // TAP_SCHEMA.columns
                GetColumnsStatement gcs = new GetColumnsStatement(columnsTableName);
                if (ordered) {
                    gcs.setOrderBy(orderColumnsClause);
                }
                List<ColumnDesc> columnDescs = jdbc.query(gcs, new ColumnMapper());
                addColumnsToTables(tableDescs, columnDescs);

                // TAP_SCHEMA.keys
                GetKeysStatement gks = new GetKeysStatement(keysTableName);
                if (ordered) {
                    gks.setOrderBy(orderKeysClause);
                }
                List<KeyDesc> keyDescs = jdbc.query(gks, new KeyMapper());

                // TAP_SCHEMA.key_columns
                GetKeyColumnsStatement gkcs = new GetKeyColumnsStatement(keyColumnsTableName);
                if (ordered) {
                    gkcs.setOrderBy(orderKeyColumnsClause);
                }
                List<KeyColumnDesc> keyColumnDescs = jdbc.query(gkcs, new KeyColumnMapper());

                // Add the KeyColumns to the Keys.
                addKeyColumnsToKeys(keyDescs, keyColumnDescs);

                // connect foreign keys to the fromTable
                addForeignKeys(schemaDescs, keyDescs);
            }
        }

        TapSchema ret = new TapSchema();
        ret.getSchemaDescs().addAll(schemaDescs);
        ret.getFunctionDescs().addAll(getFunctionDescs());
        return ret;
    }

    /**
     * Get schema description (shallow).
     * 
     * @param schemaName
     * @param depth 0 for schema, 1 for schema+tables, 2 for everything
     * @return specified SchemaDesc or null
     */
    public SchemaDesc getSchema(String schemaName, int depth) {
        GetSchemasStatement gss = new GetSchemasStatement(schemasTableName);
        gss.setSchemaName(schemaName);
        IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        log.debug("IdentityManager: " + identityManager);
        TapPermissionsMapper tapPermissionsMapper = new TapPermissionsMapper(identityManager);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<SchemaDesc> schemaDescs = jdbc.query(gss, new SchemaMapper(tapPermissionsMapper));
        if (schemaDescs.isEmpty()) {
            return null;
        }
        SchemaDesc ret = null;
        if (schemaDescs.size() == 1) {
            ret = schemaDescs.get(0);
        } else {
            throw new RuntimeException("BUG: found " + schemaDescs.size() + " schema matching " + schemaName);
        }
        if (depth > MIN_DEPTH) {
            // TAP_SCHEMA.tables
            GetTablesStatement gts = new GetTablesStatement(tablesTableName);
            gts.setSchemaName(schemaName);
            if (ordered) {
                gts.setOrderBy(orderTablesClause);
            }
            List<TableDesc> tableDescs = jdbc.query(gts, new TableMapper(tapPermissionsMapper));
            addTablesToSchemas(schemaDescs, tableDescs);
        }
        
        return ret;
    }

    /**
     * Get table description (complete).
     * 
     * @param tableName
     * @return table description or null if not found
     */
    public TableDesc getTable(String tableName) {
        return getTable(tableName, MAX_DEPTH);
    }

    /**
     * Get table description.
     * 
     * @param tableName
     * @param depth
     * @return
     */
    public TableDesc getTable(String tableName, int depth) {
        final Profiler prof = new Profiler(TapSchemaDAO.class);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        GetTablesStatement gts = new GetTablesStatement(tablesTableName);
        gts.setTableName(tableName);
        if (ordered) {
            gts.setOrderBy(orderTablesClause);
        }
        List<TableDesc> tableDescs = jdbc.query(gts, new TableMapper(null));
        if (tableDescs.isEmpty()) {
            return null;
        }
        TableDesc ret = tableDescs.get(0);
        prof.checkpoint("get-table");

        if (depth > TAB_DEPTH) {
            // TAP_SCHEMA.columns
            GetColumnsStatement gcs = new GetColumnsStatement(columnsTableName);
            gcs.setTableName(tableName);
            if (ordered) {
                gcs.setOrderBy(orderColumnsClause);
            }
            List<ColumnDesc> columnDescs = jdbc.query(gcs, new ColumnMapper());
            ret.getColumnDescs().addAll(columnDescs);
            prof.checkpoint("get-columns");

            // foreign keys
            GetKeysStatement gks = new GetKeysStatement(keysTableName);
            gks.setTableName(tableName);
            if (ordered) {
                gks.setOrderBy(orderKeysClause);
            }
            List<KeyDesc> keyDescs = jdbc.query(gks, new KeyMapper());
            prof.checkpoint("get-keys");

            // TAP_SCHEMA.key_columns
            GetKeyColumnsStatement gkcs = new GetKeyColumnsStatement(keyColumnsTableName);
            gkcs.setKeyDescs(keyDescs); // get keys for tableName only
            if (ordered) {
                gkcs.setOrderBy(orderKeyColumnsClause);
            }
            List<KeyColumnDesc> keyColumnDescs = jdbc.query(gkcs, new KeyColumnMapper());
            prof.checkpoint("get-key-columns");

            addKeyColumnsToKeys(keyDescs, keyColumnDescs);
            ret.getKeyDescs().addAll(keyDescs);
        }

        log.debug("found: " + ret);
        prof.checkpoint("get-table-done");
        return ret;
    }

    /**
     * Get a column description.
     * 
     * @param tableName
     * @param columnName
     * @return
     */
    public ColumnDesc getColumn(String tableName, String columnName) {

        GetColumnsStatement gcs = new GetColumnsStatement(columnsTableName);
        gcs.setTableName(tableName);
        gcs.setColumnName(columnName);

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<ColumnDesc> columnDescs = jdbc.query(gcs, new ColumnMapper());
        if (columnDescs.isEmpty()) {
            return null;
        }
        if (columnDescs.size() == 1) {
            return columnDescs.get(0);
        }
        throw new RuntimeException(
                "BUG: found " + columnDescs.size() + " columns matching " + tableName + " " + columnName);
    }

    public static void checkMismatchedColumnSet(TableDesc cur, TableDesc td) {
        // detect mismatched column list
        Set<String> curCols = new TreeSet<>();
        for (ColumnDesc cd : cur.getColumnDescs()) {
            log.debug("update: cur = " + cd.getColumnName());
            curCols.add(cd.getColumnName());
        }
        Set<String> tdCols = new TreeSet<>();
        for (ColumnDesc cd : td.getColumnDescs()) {
            log.debug("update: td = " + cd.getColumnName());
            tdCols.add(cd.getColumnName());
        }
        log.debug("update: " + curCols.size() + " vs " + tdCols.size());
        if (curCols.size() != tdCols.size() || !curCols.containsAll(tdCols) || !tdCols.containsAll(curCols)) {
            throw new UnsupportedOperationException("cannot add/remove/rename columns");
        }
    }

    public void put(SchemaDesc sd) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            SchemaDesc cur = getSchema(sd.getSchemaName(), 0);
            boolean update = (cur != null);
            tm.startTransaction();

            PutSchemaStatement sps = new PutSchemaStatement(update);
            log.debug("put: " + sd.getSchemaName());
            sps.setSchema(sd);
            jdbc.update(sps);

            log.debug("commit transaction");
            tm.commitTransaction();
            log.debug("commit transaction: OK");
        } catch (UnsupportedOperationException rethrow) {
            throw rethrow;
        } catch (Exception ex) {
            try {
                log.error("PUT failed - rollback", ex);
                tm.rollbackTransaction();
                log.error("PUT failed - rollback: OK");
            } catch (Exception oops) {
                log.error("PUT failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to persist " + sd.getSchemaName(), ex);
        } finally {
            if (tm.isOpen()) {
                log.error("BUG: open transaction in finally - trying to rollback");
                try {
                    tm.rollbackTransaction();
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    /**
     * Insert or update a table and columns. This does not support add/remove/rename
     * of columns in a table
     * 
     * @param td
     */
    public void put(TableDesc td) {
        final Profiler prof = new Profiler(TapSchemaDAO.class);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            TableDesc cur = getTable(td.getTableName());
            final boolean update = (cur != null);
            
            if (cur != null) {
                // add/remove/rename columns not supported
                checkMismatchedColumnSet(cur, td);
            }
            
            tm.startTransaction();
            prof.checkpoint("start-transaction");

            PutTableStatement pts = new PutTableStatement(update);
            log.debug("put: " + td.getTableName());
            pts.setTable(td);
            jdbc.update(pts);
            prof.checkpoint("put-table");

            // add/remove/rename columns not supported so update flag is same for the table and
            // column(s)
            PutColumnStatement pcs = new PutColumnStatement(update);
            for (ColumnDesc cd : td.getColumnDescs()) {
                log.debug("put: " + cd.getColumnName());
                pcs.setColumn(cd);
                jdbc.update(pcs);
            }
            prof.checkpoint("put-columns");

            log.debug("commit transaction");
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
            log.debug("commit transaction: OK");
        } catch (UnsupportedOperationException rethrow) {
            throw rethrow;
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
            throw new RuntimeException("failed to persist " + td.getTableName(), ex);
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
    }

    public void put(ColumnDesc cd) {
        final Profiler prof = new Profiler(TapSchemaDAO.class);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            TableDesc ctab = getTable(cd.getTableName());
            if (ctab == null) {
                // can only update an existing table
                throw new ResourceNotFoundException("not found: table " + cd.getTableName());
            }

            ColumnDesc col = ctab.getColumn(cd.getColumnName());
            if (col == null) {
                // can only update an existing column
                throw new ResourceNotFoundException(
                        "not found: table " + cd.getTableName() + " column " + cd.getColumnName());
            }

            tm.startTransaction();
            prof.checkpoint("start-transaction");

            // update single column
            PutColumnStatement pcs = new PutColumnStatement(true);
            log.debug("put: " + cd.getColumnName());
            pcs.setColumn(cd);
            jdbc.update(pcs);
            prof.checkpoint("put-column");

            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (UnsupportedOperationException rethrow) {
            throw rethrow;
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
            throw new RuntimeException("failed to persist " + cd.getColumnName(), ex);
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
    }

    public void deleteSchema(String schemaName) {
        final Profiler prof = new Profiler(TapSchemaDAO.class);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            SchemaDesc cur = getSchema(schemaName, TAB_DEPTH);
            if (cur == null) {
                throw new ResourceNotFoundException("not found: " + schemaName);
            }
            if (!cur.getTableDescs().isEmpty()) {
                throw new UnsupportedOperationException("cannot delete " + schemaName
                    + " reason: it contains " + cur.getTableDescs().size() + " tables");
            }
            
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            DeleteSchemaStatement dss = new DeleteSchemaStatement();
            dss.setSchema(cur);
            jdbc.update(dss);
            prof.checkpoint("delete-schema");
            
            tm.commitTransaction();
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
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    /**
     * Delete a table. This also deletes columns and keys associated with the table.
     * 
     * @param tableName
     * @throws ResourceNotFoundException
     */
    public void delete(String tableName) throws ResourceNotFoundException {
        final Profiler prof = new Profiler(TapSchemaDAO.class);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            TableDesc cur = getTable(tableName, MAX_DEPTH);
            if (cur == null) {
                throw new ResourceNotFoundException("not found: " + tableName);
            }

            tm.startTransaction();
            prof.checkpoint("start-transaction");

            // delete all columns
            DeleteColumnsStatement dcs = new DeleteColumnsStatement();
            log.debug("delete columns: " + cur.getTableName());
            dcs.setTable(cur);
            jdbc.update(dcs);
            prof.checkpoint("delete-columns");

            // delete table
            DeleteTableStatement dts = new DeleteTableStatement();
            log.debug("delete table: " + cur.getTableName());
            dts.setTable(cur);
            jdbc.update(dts);
            prof.checkpoint("delete-table");

            log.debug("commit transaction");
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
            log.debug("commit transaction: OK");
        } catch (ResourceNotFoundException rethrow) {
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
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    /**
     * Return the permissions of the schmema identified by schemaName
     * 
     * @param schemaName
     * @return An object holding the permissions for the schema.
     */
    public TapPermissions getSchemaPermissions(String schemaName) {

        GetPermissionsStatement gsp = new GetPermissionsStatement(schemasTableName, "schema_name", schemaName);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        log.debug("IdentityManager: " + identityManager);
        TapPermissionsMapper tapPermissionsMapper = new TapPermissionsMapper(identityManager);

        List<TapPermissions> tps = jdbc.query(gsp, tapPermissionsMapper);
        if (tps.isEmpty()) {
            return null;
        }
        if (tps.size() == 1) {
            return tps.get(0);
        }
        throw new RuntimeException("BUG: found " + tps.size() + " schema matching " + schemaName);
    }

    /**
     * Return the permissions of the table identified by tableName
     * 
     * @param tableName
     * @return An object holding the permissions for the table.
     */
    public TapPermissions getTablePermissions(String tableName) {

        GetPermissionsStatement gtp = new GetPermissionsStatement(tablesTableName, "table_name", tableName);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        log.debug("IdentityManager: " + identityManager);
        TapPermissionsMapper tapPermissionsMapper = new TapPermissionsMapper(identityManager);

        List<TapPermissions> tps = jdbc.query(gtp, tapPermissionsMapper);
        if (tps.isEmpty()) {
            return null;
        }
        if (tps.size() == 1) {
            return tps.get(0);
        }
        throw new RuntimeException("BUG: found " + tps.size() + " tables matching " + tableName);
    }

    /**
     * Set the permissions of the schema identified by schemaName. See TapPermissions 
     * for how null values of the permissions field are handled.
     * 
     * @param schemaName
     * @param tp
     * @throws ResourceNotFoundException
     */
    public void setSchemaPermissions(String schemaName, TapPermissions tp) throws ResourceNotFoundException {
        if (tp.owner != null) {
            IdentityManager im = AuthenticationUtil.getIdentityManager();
            tp.ownerID = im.toOwner(tp.owner);
            // allow clear permissions but detectn failure to extract owner ident
            if (!tp.owner.getPrincipals().isEmpty() && tp.ownerID == null) {
                throw new UnsupportedOperationException("unable to map schema owner '"
                        + im.toDisplayString(tp.owner) + "' to a persistent owner object using " + im.getClass().getName());
            }
        }
        PutPermissionsStatement ssp = new PutPermissionsStatement(schemasTableName, "schema_name", schemaName, tp);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        int rows = jdbc.update(ssp);
        if (rows == 0) {
            throw new ResourceNotFoundException("No schema named " + schemaName);
        }
        if (rows != 1) {
            throw new RuntimeException("BUG: found " + rows + " schema matching " + schemaName);
        }
    }

    /**
     * Set the permissions of the schema identified by tableName. See TapPermissions 
     * for how null values of the permissions field are handled.
     * 
     * @param tableName
     * @param tp
     * @throws ResourceNotFoundException
     */
    public void setTablePermissions(String tableName, TapPermissions tp) throws ResourceNotFoundException {
        if (tp.owner != null) {
            IdentityManager im = AuthenticationUtil.getIdentityManager();
            tp.ownerID = im.toOwner(tp.owner);
            // allow clear permissions but detectn failure to extract owner ident
            if (!tp.owner.getPrincipals().isEmpty() && tp.ownerID == null) {
                throw new UnsupportedOperationException("unable to map schema owner '"
                        + im.toDisplayString(tp.owner) + "' to a persistent owner object using " + im.getClass().getName());
            }
        }
        // update tables permissions
        PutPermissionsStatement stp = new PutPermissionsStatement(tablesTableName, "table_name", tableName, tp);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        int rows = jdbc.update(stp);
        if (rows == 0) {
            throw new ResourceNotFoundException("No table named " + tableName);
        }
        if (rows != 1) {
            throw new RuntimeException("BUG: found " + rows + " table matching " + tableName);
        }
    }

    private String toCommaList(String[] strs, int numUpdateKeys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strs.length - numUpdateKeys; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(strs[i]);
        }
        return sb.toString();
    }

    private String toParamList(String[] strs, int numUpdateKeys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strs.length - numUpdateKeys; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
        return sb.toString();
    }

    private class GetSchemasStatement implements PreparedStatementCreator {
        private String schemasTab;
        private String schemaName;
        private String orderBy;

        public GetSchemasStatement(String schemasTab) {
            this.schemasTab = schemasTab;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public void setOrderBy(String orderBy) {
            this.orderBy = orderBy;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsSchemaCols, 0));
            sb.append(",").append(toCommaList(accessControlCols, 0));
            sb.append(" FROM ").append(schemasTab);

            if (schemaName != null) {
                sb.append(" WHERE schema_name = ?");
            }
            if (orderBy != null) {
                sb.append(orderBy);
            }

            String sql = sb.toString();
            log.debug(sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            int paramIndex = 1;

            if (schemaName != null) {
                prep.setString(paramIndex++, schemaName);
            }

            return prep;
        }
    }

    private class GetTablesStatement implements PreparedStatementCreator {
        private String tablesTab;
        private String schemaName;
        private String tableName;
        private String orderBy;

        public GetTablesStatement(String tablesTab) {
            this.tablesTab = tablesTab;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setOrderBy(String orderBy) {
            this.orderBy = orderBy;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsTablesCols, 0));
            sb.append(",").append(toCommaList(accessControlCols, 0));
            sb.append(" FROM ").append(tablesTab);

            String wa = " WHERE";
            if (schemaName != null) {
                sb.append(wa).append(" schema_name = ?");
                wa = " AND";
            }
            if (tableName != null) {
                sb.append(wa).append(" table_name = ?");
            }
            if (orderBy != null) {
                sb.append(orderBy);
            }

            String sql = sb.toString();
            log.debug(sql);

            StringBuilder vals = new StringBuilder();
            vals.append("values: ");

            PreparedStatement prep = conn.prepareStatement(sql);
            int paramIndex = 1;

            if (schemaName != null) {
                prep.setString(paramIndex++, schemaName);
                vals.append(schemaName);
            }
            if (tableName != null) {
                prep.setString(paramIndex++, tableName);
                vals.append(tableName);
            }
            log.debug("values: " + vals.toString());
            return prep;
        }
    }

    private class GetColumnsStatement implements PreparedStatementCreator {
        private String columnsTab;
        private String tableName;
        private String columnName;
        private String orderBy;

        public GetColumnsStatement(String columnsTab) {
            this.columnsTab = columnsTab;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public void setOrderBy(String orderBy) {
            this.orderBy = orderBy;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsColumnsCols, 0));
            sb.append(" FROM ").append(columnsTab);

            if (tableName != null) {
                sb.append(" WHERE table_name = ?");
                if (columnName != null) {
                    sb.append(" AND column_name = ?");
                }
            }
            if (orderBy != null) {
                sb.append(orderBy);
            }

            String sql = sb.toString();
            log.debug(sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            int paramIndex = 1;

            if (tableName != null) {
                prep.setString(paramIndex++, tableName);
                if (columnName != null) {
                    prep.setString(paramIndex++, columnName);
                }
            }
            return prep;
        }
    }

    private class GetKeysStatement implements PreparedStatementCreator {
        private String keysTab;
        private String tableName;
        private String orderBy;

        public GetKeysStatement(String keysTab) {
            this.keysTab = keysTab;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setOrderBy(String orderBy) {
            this.orderBy = orderBy;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsKeysCols, 0));
            sb.append(" FROM ").append(keysTab);
            if (tableName != null) {
                sb.append(" WHERE from_table = ?");
            } else if (orderBy != null) {
                sb.append(orderBy);
            }

            String sql = sb.toString();
            log.debug(sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            if (tableName != null) {
                prep.setString(1, tableName);
            }
            return prep;
        }
    }

    private class GetKeyColumnsStatement implements PreparedStatementCreator {
        private String keyColumnsTab;
        private List<KeyDesc> keyDescs;
        private String orderBy;

        public GetKeyColumnsStatement(String keyColumnsTab) {
            this.keyColumnsTab = keyColumnsTab;
        }

        public void setKeyDescs(List<KeyDesc> keyDescs) {
            this.keyDescs = keyDescs;
        }

        public void setOrderBy(String orderBy) {
            this.orderBy = orderBy;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsKeyColumnsCols, 0));
            sb.append(" FROM ").append(keyColumnsTab);

            if (keyDescs != null && !keyDescs.isEmpty()) {
                sb.append(" WHERE key_id IN (");
                for (int i = 0; i < keyDescs.size(); i++) {
                    sb.append("?,");
                }
                sb.setCharAt(sb.length() - 1, ')'); // replace last | with closed bracket
            } else if (orderBy != null) {
                sb.append(orderBy);
            }

            String sql = sb.toString();
            log.debug(sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            if (keyDescs != null && !keyDescs.isEmpty()) {
                int col = 1;
                for (KeyDesc kd : keyDescs) {
                    log.debug("values: " + kd.getKeyID());
                    prep.setString(col++, kd.getKeyID());
                }
            }
            return prep;
        }
    }

    private class GetPermissionsStatement implements PreparedStatementCreator {
        private String table;
        private String column;
        private String name;
        private TapPermissions tp;

        public GetPermissionsStatement(String table, String column, String name) {
            this.table = table;
            this.column = column;
            this.name = name;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(ownerCol).append(", ");
            sb.append(readAnonCol).append(", ");
            sb.append(readOnlyCol).append(", ");
            sb.append(readWriteCol);
            sb.append(" FROM ").append(table);
            sb.append(" WHERE ").append(column).append(" = ?");
            String sql = sb.toString();
            log.debug("sql: " + sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            StringBuilder vals = new StringBuilder();
            vals.append("values: ");
            safeSetString(vals, prep, 1, name);
            log.debug(vals.toString());
            return prep;
        }

    }
    
    private class PutPermissionsStatement implements PreparedStatementCreator {
        private String table;
        private String column;
        private String name;
        private TapPermissions tp;

        public PutPermissionsStatement(String table, String column, String name, TapPermissions tp) {
            this.table = table;
            this.column = column;
            this.name = name;
            this.tp = tp;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            int colIndex = 1;
            sb.append("UPDATE ");
            sb.append(table);
            sb.append(" SET (");
            sb.append(ownerCol).append(", ");
            sb.append(readAnonCol).append(", ");
            sb.append(readOnlyCol).append(", ");
            sb.append(readWriteCol);
            sb.append(")");
            sb.append(" = (?, ?, ?, ?)");
            sb.append(" WHERE ").append(column).append(" = ?");

            String sql = sb.toString();
            log.debug("sql: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);

            StringBuilder vals = new StringBuilder();
            vals.append("values: ");

            if (tp.ownerID != null) {
                safeSetString(vals, prep, colIndex++, tp.ownerID.toString());
            } else {
                safeSetString(vals, prep, colIndex++, null);
            }
            safeSetBoolean(vals, prep, colIndex++, tp.isPublic);
            safeSetURI(vals, prep, colIndex++, tp.readGroup);
            safeSetURI(vals, prep, colIndex++, tp.readWriteGroup);

            safeSetString(vals, prep, colIndex++, name);

            log.debug(vals.toString());

            return prep;
        }

    }

    private class PutSchemaStatement implements PreparedStatementCreator {
        private SchemaDesc schema;
        private final boolean update;

        PutSchemaStatement(boolean update) {
            this.update = update;
        }

        public void setSchema(SchemaDesc schema) {
            this.schema = schema;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            if (update) {
                sb.append("UPDATE ").append(schemasTableName);
                sb.append(" SET (");
                sb.append(toCommaList(tsSchemaCols, 1));
                sb.append(") = (");
                sb.append(toParamList(tsSchemaCols, 1));
                sb.append(")");
                sb.append(" WHERE schema_name=?");
            } else {
                sb.append("INSERT INTO ").append(schemasTableName);
                sb.append(" (");
                sb.append(toCommaList(tsSchemaCols, 0));
                sb.append(") VALUES (");
                sb.append(toParamList(tsSchemaCols, 0));
                sb.append(")");
            }
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            // load values: description, utype, schema_index, schema_name, api_created
            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, schema.description);
            safeSetString(sb, ps, col++, schema.utype);
            safeSetInteger(sb, ps, col++, schema.schemaIndex);
            safeSetBoolean(sb, ps, col++, schema.apiCreated);
            safeSetString(sb, ps, col++, schema.getSchemaName());

            return ps;
        }
    }

    private class PutTableStatement implements PreparedStatementCreator {
        private TableDesc table;
        private final boolean update;

        PutTableStatement(boolean update) {
            this.update = update;
        }

        public void setTable(TableDesc table) {
            this.table = table;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            if (update) {
                sb.append("UPDATE ").append(tablesTableName);
                sb.append(" SET (");
                sb.append(toCommaList(tsTablesCols, 1));
                sb.append(") = (");
                sb.append(toParamList(tsTablesCols, 1));
                sb.append(")");
                sb.append(" WHERE table_name=?");
            } else {
                sb.append("INSERT INTO ").append(tablesTableName);
                sb.append(" (");
                sb.append(toCommaList(tsTablesCols, 0));
                sb.append(") VALUES (");
                sb.append(toParamList(tsTablesCols, 0));
                sb.append(")");
            }
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            // load values: description, utype, schema_name, table_name, api_created
            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, table.getSchemaName());
            safeSetString(sb, ps, col++, table.tableType.getValue());
            safeSetString(sb, ps, col++, table.description);
            safeSetString(sb, ps, col++, table.utype);
            safeSetInteger(sb, ps, col++, table.tableIndex);
            safeSetBoolean(sb, ps, col++, table.apiCreated);
            safeSetString(sb, ps, col++, table.getTableName());

            return ps;
        }
    }

    private class PutColumnStatement implements PreparedStatementCreator {
        private ColumnDesc column;
        private boolean update;

        public PutColumnStatement(boolean update) {
            this.update = update;
        }

        public void setColumn(ColumnDesc column) {
            this.column = column;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            if (update) {
                sb.append("UPDATE ").append(columnsTableName);
                sb.append(" SET (");
                sb.append(toCommaList(tsColumnsCols, 2));
                sb.append(") = (");
                sb.append(toParamList(tsColumnsCols, 2));
                sb.append(")");
                sb.append(" WHERE table_name=? AND column_name=?");
            } else {
                sb.append("INSERT INTO ").append(columnsTableName);
                sb.append(" (");
                sb.append(toCommaList(tsColumnsCols, 0));
                sb.append(") VALUES (");
                sb.append(toParamList(tsColumnsCols, 0));
                sb.append(")");
            }
            String sql = sb.toString();
            log.debug("sql: " + sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            // description, utype, ucd, unit, datatype, arraysize, xtype, principal,
            // indexed, std, column_id,
            // table_name, column_name
            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, column.description);
            safeSetString(sb, ps, col++, column.utype);
            safeSetString(sb, ps, col++, column.ucd);
            safeSetString(sb, ps, col++, column.unit);
            safeSetString(sb, ps, col++, column.getDatatype().getDatatype());
            safeSetString(sb, ps, col++, column.getDatatype().arraysize);
            safeSetString(sb, ps, col++, column.getDatatype().xtype);
            safeSetBoolean(sb, ps, col++, column.principal);
            safeSetBoolean(sb, ps, col++, column.indexed);
            safeSetBoolean(sb, ps, col++, column.std);
            safeSetString(sb, ps, col++, column.columnID);
            safeSetInteger(sb, ps, col++, column.columnIndex);
            safeSetString(sb, ps, col++, column.getTableName());
            safeSetString(sb, ps, col++, column.getColumnName());
            log.debug("values: " + sb.toString());

            return ps;
        }
    }

    private class DeleteSchemaStatement implements PreparedStatementCreator {
        private SchemaDesc schema;

        public void setSchema(SchemaDesc schema) {
            this.schema = schema;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(schemasTableName);
            sb.append(" WHERE schema_name=?");
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, schema.getSchemaName());
            log.debug("values: " + sb.toString());

            return ps;
        }
    }
    
    private class DeleteTableStatement implements PreparedStatementCreator {
        private TableDesc table;

        public void setTable(TableDesc table) {
            this.table = table;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(tablesTableName);
            sb.append(" WHERE table_name=?");
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, table.getTableName());
            log.debug("values: " + sb.toString());

            return ps;
        }
    }

    private class DeleteColumnsStatement implements PreparedStatementCreator {
        private TableDesc table;

        public void setTable(TableDesc table) {
            this.table = table;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(columnsTableName);
            sb.append(" WHERE table_name=?");
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, table.getTableName());
            log.debug("values: " + sb.toString());

            return ps;
        }
    }

    private class DeleteColumnStatement implements PreparedStatementCreator {
        private ColumnDesc column;

        public void setColumn(ColumnDesc column) {
            this.column = column;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(columnsTableName);
            sb.append(" WHERE table_name=? AND column_name=?");
            String sql = sb.toString();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            // load values
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Creates Lists of Tables with a common Schema name, then adds the Lists to the
     * Schemas.
     * 
     * @param schemaDescs
     *            List of Schemas.
     * @param tableDescs
     *            List of Tables.
     */
    private void addTablesToSchemas(List<SchemaDesc> schemaDescs, List<TableDesc> tableDescs) {
        for (TableDesc tableDesc : tableDescs) {
            for (SchemaDesc schemaDesc : schemaDescs) {
                if (tableDesc.getSchemaName().equals(schemaDesc.getSchemaName())) {
                    schemaDesc.getTableDescs().add(tableDesc);
                    break;
                }
            }
        }
    }

    /**
     * Creates Lists of Columns with a common Table name, then adds the Lists to the
     * Tables.
     * 
     * @param tableDescs
     *            List of Tables.
     * @param columnDescs
     *            List of Columns.
     */
    private void addColumnsToTables(List<TableDesc> tableDescs, List<ColumnDesc> columnDescs) {
        for (ColumnDesc col : columnDescs) {
            for (TableDesc tableDesc : tableDescs) {
                if (col.getTableName().equals(tableDesc.getTableName())) {
                    tableDesc.getColumnDescs().add(col);
                    break;
                }
            }
        }
    }

    /**
     * Creates Lists of KeyColumns with a common Key keyID, then adds the Lists to
     * the Keys.
     * 
     * @param keyDescs
     *            List of Keys.
     * @param keyColumnDescs
     *            List of KeyColumns.
     */
    private void addKeyColumnsToKeys(List<KeyDesc> keyDescs, List<KeyColumnDesc> keyColumnDescs) {
        for (KeyColumnDesc keyColumnDesc : keyColumnDescs) {
            for (KeyDesc keyDesc : keyDescs) {
                if (keyColumnDesc.getKeyID().equals(keyDesc.getKeyID())) {
                    keyDesc.getKeyColumnDescs().add(keyColumnDesc);
                    break;
                }
            }
        }
    }

    /**
     * Adds foreign keys (KeyDesc) to the from table.
     * 
     * @param schemaDescs
     */
    private void addForeignKeys(List<SchemaDesc> schemaDescs, List<KeyDesc> keyDescs) {
        for (KeyDesc key : keyDescs) {
            for (SchemaDesc sd : schemaDescs) {
                for (TableDesc td : sd.getTableDescs()) {
                    if (key.getFromTable().equals(td.getTableName())) {
                        td.getKeyDescs().add(key);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Get white-list of supported functions. TAP implementors that want to allow
     * additional functions to be used in queries to be used should override this
     * method, call <code>super.getFunctionDescs()</code>, and then add additional
     * FunctionDesc descriptors to the list before returning it.
     *
     * @return white list of allowed functions
     */
    protected List<FunctionDesc> getFunctionDescs() {
        List<FunctionDesc> functionDescs = new ArrayList<FunctionDesc>();

        // ADQL functions.
        functionDescs.add(new FunctionDesc("AREA", TapDataType.DOUBLE, "deg**2"));
        // functionDescs.add(new FunctionDesc("BOX", new TapDataType("adql:REGION",
        // null, null, null)));
        functionDescs.add(new FunctionDesc("CENTROID", TapDataType.POINT));
        functionDescs.add(new FunctionDesc("CIRCLE", TapDataType.CIRCLE));
        functionDescs.add(new FunctionDesc("CONTAINS", TapDataType.INTEGER));
        functionDescs.add(new FunctionDesc("COORD1", TapDataType.DOUBLE, "deg"));
        functionDescs.add(new FunctionDesc("COORD2", TapDataType.DOUBLE, "deg"));
        functionDescs.add(new FunctionDesc("COORDSYS", new TapDataType("char", "16*", null)));
        functionDescs.add(new FunctionDesc("DISTANCE", TapDataType.DOUBLE, "deg"));
        functionDescs.add(new FunctionDesc("INTERSECTS", TapDataType.INTEGER));
        functionDescs.add(new FunctionDesc("INTERVAL", TapDataType.INTERVAL));
        functionDescs.add(new FunctionDesc("POINT", TapDataType.POINT));
        functionDescs.add(new FunctionDesc("POLYGON", TapDataType.POLYGON));
        // functionDescs.add(new FunctionDesc("REGION", new TapDataType("adql:REGION",
        // null, null, null)));

        // ADQL reserved keywords that are functions.
        functionDescs.add(new FunctionDesc("ABS", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("ACOS", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("ASIN", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("ATAN", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("ATAN2", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("CEILING", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("COS", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("COT", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("DEGREES", TapDataType.DOUBLE, "deg"));
        functionDescs.add(new FunctionDesc("EXP", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("FLOOR", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("LN", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("LOG", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("LOG10", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("MOD", TapDataType.FUNCTION_ARG));
        /*
         * Part of the ADQL BNF, but currently not parseable pending bug fix in the
         * jsqlparser.
         *
         * functionDescs.add(new FunctionDesc("PI", "", "adql:DOUBLE"));
         */
        functionDescs.add(new FunctionDesc("POWER", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("RADIANS", TapDataType.DOUBLE, "radians"));
        /*
         * Part of the ADQL BNF, but currently not parseable pending bug fix in the
         * jsqlparser.
         *
         * functionDescs.add(new FunctionDesc("RAND", "", "adql:DOUBLE"));
         */
        functionDescs.add(new FunctionDesc("ROUND", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("SIN", TapDataType.DOUBLE, "radians"));
        functionDescs.add(new FunctionDesc("SQRT", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("TAN", TapDataType.DOUBLE, "radians"));
        /*
         * Part of the ADQL BNF, but currently not parseable.
         *
         * functionDescs.add(new FunctionDesc("TRUNCATE", "", "adql:DOUBLE"));
         */

        // SQL Aggregate functions.
        functionDescs.add(new FunctionDesc("AVG", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("COUNT", new TapDataType("long", null, null)));
        functionDescs.add(new FunctionDesc("MAX", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("MIN", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("STDDEV", TapDataType.DOUBLE));
        functionDescs.add(new FunctionDesc("SUM", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("VARIANCE", TapDataType.DOUBLE));

        // SQL String functions.
        // functionDescs.add(new FunctionDesc("BIT_LENGTH", "", "adql:INTEGER"));
        // functionDescs.add(new FunctionDesc("CHARACTER_LENGTH", "", "adql:INTEGER"));
        functionDescs.add(new FunctionDesc("LOWER", new TapDataType("char", "*", null)));
        // functionDescs.add(new FunctionDesc("OCTET_LENGTH", "", "adql:INTEGER"));
        // functionDescs.add(new FunctionDesc("OVERLAY", "", "adql:VARCHAR"));
        // //SQL92???
        // functionDescs.add(new FunctionDesc("POSITION", "", "adql:INTEGER"));
        functionDescs.add(new FunctionDesc("SUBSTRING", new TapDataType("char", "*", null)));
        // functionDescs.add(new FunctionDesc("TRIM", new TapDataType("char", "*",
        // null)));
        functionDescs.add(new FunctionDesc("UPPER", new TapDataType("char", "*", null)));

        // SQL Date functions.
        // functionDescs.add(new FunctionDesc("CURRENT_DATE", "", "adql:TIMESTAMP"));
        // functionDescs.add(new FunctionDesc("CURRENT_TIME", "", "adql:TIMESTAMP"));
        // functionDescs.add(new FunctionDesc("CURRENT_TIMESTAMP", "",
        // "adql:TIMESTAMP"));
        // functionDescs.add(new FunctionDesc("EXTRACT", "", "adql:TIMESTAMPs"));
        // functionDescs.add(new FunctionDesc("LOCAL_DATE", "", "adql:TIMESTAMP"));
        // //SQL92???
        // functionDescs.add(new FunctionDesc("LOCAL_TIME", "", "adql:TIMESTAMP"));
        // //SQL92???
        // functionDescs.add(new FunctionDesc("LOCAL_TIMESTAMP", "", "adql:TIMESTAMP"));
        // //SQL92???

        return functionDescs;
    }

    /**
     * Creates a List of Schema populated from the ResultSet.
     */
    private static final class SchemaMapper implements RowMapper {
        TapPermissionsMapper tapPermissionsMapper;

        public SchemaMapper(TapPermissionsMapper tapPermissionsMapper) {
            this.tapPermissionsMapper = tapPermissionsMapper;
        }

        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String sn = rs.getString("schema_name");
            SchemaDesc schemaDesc = new SchemaDesc(sn);

            schemaDesc.description = rs.getString("description");
            schemaDesc.utype = rs.getString("utype");
            schemaDesc.schemaIndex = rs.getInt("schema_index");
            schemaDesc.apiCreated = rs.getInt("api_created") == 1;
            
            if (tapPermissionsMapper != null) {
                schemaDesc.tapPermissions = tapPermissionsMapper.mapRow(rs, rowNum);
            }

            return schemaDesc;
        }
    }

    /**
     * Creates a List of Table populated from the ResultSet.
     */
    private static final class TableMapper implements RowMapper {
        TapPermissionsMapper tapPermissionsMapper;

        public TableMapper(TapPermissionsMapper tapPermissionsMapper) {
            this.tapPermissionsMapper = tapPermissionsMapper;
        }

        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String sn = rs.getString("schema_name");
            String tn = rs.getString("table_name");
            TableDesc tableDesc = new TableDesc(sn, tn);

            tableDesc.tableType = TableDesc.TableType.toValue(rs.getString("table_type"));
            tableDesc.description = rs.getString("description");
            tableDesc.utype = rs.getString("utype");
            tableDesc.tableIndex = rs.getInt("table_index");
            tableDesc.apiCreated = rs.getInt("api_created") == 1;
            
            if (tapPermissionsMapper != null) {
                tableDesc.tapPermissions = tapPermissionsMapper.mapRow(rs, rowNum);
            }
            
            return tableDesc;
        }
    }

    /**
     * Creates a List of Column populated from the ResultSet.
     */
    private static final class ColumnMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String tn = rs.getString("table_name");
            String cn = rs.getString("column_name");
            String dt = rs.getString("datatype");
            String as = rs.getString("arraysize");
            String xt = rs.getString("xtype");

            log.debug("ColumnMapper: " + tn + "," + cn + "," + dt + "," + as + "," + xt);

            TapDataType datatype = new TapDataType(dt, as, xt);
            ColumnDesc col = new ColumnDesc(tn, cn, datatype);

            col.description = rs.getString("description");
            col.utype = rs.getString("utype");
            col.ucd = rs.getString("ucd");
            col.unit = rs.getString("unit");

            col.principal = intToBoolean(rs.getInt("principal"));
            col.indexed = intToBoolean(rs.getInt("indexed"));
            col.std = intToBoolean(rs.getInt("std"));
            col.columnID = rs.getString("column_id");
            col.columnIndex = rs.getInt("column_index");
            
            return col;
        }

        private boolean intToBoolean(Integer i) {
            if (i == null) {
                return false;
            }
            return (i == 1);
        }
    }

    /**
     * Creates a List of Key populated from the ResultSet.
     */
    private static final class KeyMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String kid = rs.getString("key_id");
            String ft = rs.getString("from_table");
            String tt = rs.getString("target_table");
            KeyDesc keyDesc = new KeyDesc(kid, ft, tt);

            keyDesc.description = rs.getString("description");
            keyDesc.utype = rs.getString("utype");

            return keyDesc;
        }
    }

    /**
     * Creates a List of KeyColumn populated from the ResultSet.
     */
    private static final class KeyColumnMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String kid = rs.getString("key_id");
            String fc = rs.getString("from_column");
            String tc = rs.getString("target_column");
            KeyColumnDesc keyColumnDesc = new KeyColumnDesc(kid, fc, tc);

            return keyColumnDesc;
        }
    }

    /**
     * Creates a List of Schema populated from the ResultSet.
     */
    private static final class TapPermissionsMapper implements RowMapper {

        private IdentityManager identityManager;

        public TapPermissionsMapper(IdentityManager identityManager) {
            this.identityManager = identityManager;
        }

        public TapPermissions mapRow(ResultSet rs, int rowNum) throws SQLException {
            String ownerVal = rs.getString(ownerCol);
            log.debug("found owner: " + ownerVal);
            int readAnon = rs.getInt(readAnonCol);
            log.debug("found readAnon: " + readAnon);
            String rog = rs.getString(readOnlyCol);
            log.debug("found readOnly: " + rog);
            String rwg = rs.getString(readWriteCol);
            log.debug("found readAnon: " + rwg);

            Subject owner = null;
            if (ownerVal != null) {
                owner = identityManager.toSubject(ownerVal);
            }
            // a value of zero is either null or false
            boolean isPublic = readAnon != 0;
            GroupURI readGroup = null;
            GroupURI readWriteGroup = null;
            if (rog != null) {
                readGroup = new GroupURI(URI.create(rog));
            }
            if (rwg != null) {
                readWriteGroup = new GroupURI(URI.create(rwg));
            }

            TapPermissions tapPermissions = new TapPermissions(owner, isPublic, readGroup, readWriteGroup);

            return tapPermissions;
        }
    }

    /**
     * Data holder for access control SQL and values.
     */
    private static final class AccessControlSQL {
        public String sql = null;

        public int publicValue = 1;
        public String ownerValue = null;
        public List<String> groupValues = new ArrayList<String>();
    }

    protected void safeSetString(StringBuilder sb, PreparedStatement ps, int col, String val) throws SQLException {
        if (val != null) {
            ps.setString(col, val);
        } else {
            ps.setNull(col, Types.VARCHAR);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    protected void safeSetURI(StringBuilder sb, PreparedStatement ps, int col, GroupURI val) throws SQLException {
        if (val != null) {
            ps.setString(col, val.getURI().toASCIIString());
        } else {
            ps.setNull(col, Types.VARCHAR);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    protected void safeSetInteger(StringBuilder sb, PreparedStatement ps, int col, Integer val) throws SQLException {
        if (val != null) {
            ps.setLong(col, val);
        } else {
            ps.setNull(col, Types.INTEGER);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    protected void safeSetBoolean(StringBuilder sb, PreparedStatement ps, int col, Boolean val) throws SQLException {
        if (useIntegerForBoolean) {
            Integer ival = null;
            if (val != null) {
                if (val.booleanValue()) {
                    ival = new Integer(1);
                } else {
                    ival = new Integer(0);
                }
            }
            safeSetInteger(sb, ps, col, ival);
            return;
        }

        if (val != null) {
            ps.setBoolean(col, val);
        } else {
            ps.setNull(col, Types.BOOLEAN);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

}
