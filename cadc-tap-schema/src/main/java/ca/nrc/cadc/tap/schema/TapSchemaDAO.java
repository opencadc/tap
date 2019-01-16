/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2009.                            (c) 2009.
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

import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.uws.Job;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

/**
 * Given a DataSource to a TAP_SCHEMA, returns a TapSchema object containing the TAP_SCHEMA data.
 * The fully qualified names of tables in the tap_schema can be modified in a subclass as long 
 * as the change(s) are made before the get method is called (*TableName variables).
 */
public class TapSchemaDAO
{
    private static final Logger log = Logger.getLogger(TapSchemaDAO.class);

    /**
     * Integer representation of the TAP version (TAP 1.1 value is 11). This
     * value is expected to be appended to the tap_schema table names so that
     * two versions of the tap_schema tables can co-exist.
     */
    protected final int TAP_VERSION = 11;
    
    // standard tap_schema table names
    protected String schemasTableName = "tap_schema.schemas" + TAP_VERSION;
    protected String tablesTableName = "tap_schema.tables" + TAP_VERSION;
    protected String columnsTableName = "tap_schema.columns" + TAP_VERSION;
    protected String keysTableName = "tap_schema.keys" + TAP_VERSION;
    protected String keyColumnsTableName = "tap_schema.key_columns" + TAP_VERSION;

    private String[] tsSchemaCols = new String[] { "description", "utype", "schema_index", "schema_name" };
    protected String orderSchemaClause = " ORDER BY schema_name";

    private String[] tsTablesCols = new String[] { "table_type", "description", "utype", "table_index", "schema_name", "table_name" };
    protected String orderTablesClause = " ORDER BY schema_name,table_index,table_name";

    private String[] tsColumnsCols = new String[] { "description", "utype", "ucd", "unit", 
        "datatype", "arraysize", "xtype", "principal", "indexed", "std", "id", "column_index", "table_name", "column_name" };
    protected String orderColumnsClause = " ORDER BY table_name,column_index,column_name";
    
    private String[] tsKeysCols = new String[] { "key_id", "from_table", "target_table", "description,utype" };
    protected String orderKeysClause = " ORDER BY key_id,from_table,target_table";

    private String[] tsKeyColumnsCols = new String[] { "key_id", "from_column", "target_column" };
    protected String orderKeyColumnsClause = " ORDER BY key_id, from_column, target_column";

    protected Job job;
    protected DataSource dataSource;
    private boolean ordered;
    private boolean useIntegerForBoolean = true;
    
    // Indicates function return datatype matches argument datatype.
    public static final String ARGUMENT_DATATYPE = "ARGUMENT_DATATYPE";
    
    public static final int MIN_DEPTH = 0; // schema and tables only
    public static final int MAX_DEPTH = 1; // columns, keys, etc

    /**
     * Construct a new TapSchemaDAO.
     */
    public TapSchemaDAO() { }

    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    public void setJob(Job job)
    {
        this.job = job;
    }

    public Job getJob()
    {
        return job;
    }

    
    public void setOrdered(boolean ordered)
    {
        this.ordered = ordered;
    }

    /** 
     * Get the complete TapSchema.
     * 
     * @return complete TapSchema
     */
    public TapSchema get()
    {
        // depth MIN = schemas and tables
        // depth MAX = columns + keys + functions
        return get(MAX_DEPTH);
    }
    
    /**
     * Creates and returns a TapSchema object representing some or all of the content in TAP_SCHEMA.
     * 
     * @param depth use MIN_DEPTH to get schame and table names only, MAX_DEPTH to get everything
     * @return TapSchema containing some or all of the content
     */
    public TapSchema get(int depth)
    {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        // List of TAP_SCHEMA.schemas
        GetSchemasStatement gss = new GetSchemasStatement(schemasTableName);
        if (ordered)
            gss.setOrderBy(orderSchemaClause);
        List<SchemaDesc> schemaDescs = jdbc.query(gss, new SchemaMapper());
        
        // TAP_SCHEMA.tables
        GetTablesStatement gts = new GetTablesStatement(tablesTableName);
        if (ordered)
            gts.setOrderBy(orderTablesClause);
        List<TableDesc> tableDescs = jdbc.query(gts, new TableMapper());
        
        // Add the Tables to the Schemas.
        addTablesToSchemas(schemaDescs, tableDescs);
        
        // TAP_SCHEMA.columns
        if (depth > MIN_DEPTH)
        {
            GetColumnsStatement gcs = new GetColumnsStatement(columnsTableName);
            if (ordered)
                gcs.setOrderBy(orderColumnsClause);
            List<ColumnDesc> columnDescs = jdbc.query(gcs, new ColumnMapper());

            // Add the Columns to the Tables.
            addColumnsToTables(tableDescs, columnDescs);
        
            // List of TAP_SCHEMA.keys
            GetKeysStatement gks = new GetKeysStatement(keysTableName);
            if (ordered)
                gks.setOrderBy(orderKeysClause);
            List<KeyDesc> keyDescs = jdbc.query(gks, new KeyMapper());

            // TAP_SCHEMA.key_columns
            GetKeyColumnsStatement gkcs = new GetKeyColumnsStatement(keyColumnsTableName);
            if (ordered)
                gkcs.setOrderBy(orderKeyColumnsClause);
            List<KeyColumnDesc> keyColumnDescs = jdbc.query(gkcs, new KeyColumnMapper());

            // Add the KeyColumns to the Keys.
            addKeyColumnsToKeys(keyDescs, keyColumnDescs);

            // connect foreign keys to the fromTable
            addForeignKeys(schemaDescs, keyDescs);
        }
        
        TapSchema ret = new TapSchema();
        ret.getSchemaDescs().addAll(schemaDescs);
        ret.getFunctionDescs().addAll(getFunctionDescs());
        return ret;
    }
    
    // gets table+columns+keys+key_columns
    public TableDesc getTable(String tableName) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        
        GetTablesStatement gts = new GetTablesStatement(tablesTableName);
        gts.setTableName(tableName);
        if (ordered)
            gts.setOrderBy(orderTablesClause);
        List<TableDesc> tableDescs = jdbc.query(gts, new TableMapper());
        if (tableDescs.isEmpty()) {
            return null;
        }
        TableDesc ret = tableDescs.get(0);
        
        // column metadata
        GetColumnsStatement gcs = new GetColumnsStatement(columnsTableName);
        gcs.setTableName(tableName);
        if (ordered) {
            gcs.setOrderBy(orderColumnsClause);
        }
        List<ColumnDesc> columnDescs = jdbc.query(gcs, new ColumnMapper());
        ret.getColumnDescs().addAll(columnDescs);
        
        // foreign keys
        GetKeysStatement gks = new GetKeysStatement(keysTableName);
        gks.setTableName(tableName);
        if (ordered) {
            gks.setOrderBy(orderKeysClause);
        }
        List<KeyDesc> keyDescs = jdbc.query(gks, new KeyMapper());

        // TAP_SCHEMA.key_columns
        GetKeyColumnsStatement gkcs = new GetKeyColumnsStatement(keyColumnsTableName);
        gkcs.setKeyDescs(keyDescs); // get keys for tableName only
        if (ordered) {
            gkcs.setOrderBy(orderKeyColumnsClause);
        }
        List<KeyColumnDesc> keyColumnDescs = jdbc.query(gkcs, new KeyColumnMapper());

        addKeyColumnsToKeys(keyDescs, keyColumnDescs);
        ret.getKeyDescs().addAll(keyDescs);
        
        log.debug("found: " + ret);
        return ret;
    }
    
    
    // package access for intTest code only: shallow put of a schema
    SchemaDesc getSchema(String schemaName) {
        // List of TAP_SCHEMA.schemas
        GetSchemasStatement gss = new GetSchemasStatement(schemasTableName);
        if (ordered)
            gss.setOrderBy(orderSchemaClause);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<SchemaDesc> schemaDescs = jdbc.query(gss, new SchemaMapper());
        for (SchemaDesc sd : schemaDescs) {
            if (sd.getSchemaName().equals(schemaName)) {
                return sd;
            }
        }
        return null;
    }
    
    /*
    void put(SchemaDesc sd) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            SchemaDesc cur = getSchema(sd.getSchemaName());
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
    */
    
    /**
     * Insert or update a table and columns. This does not support add/remove/rename of columns
     * in a table
     * @param td 
     */
    public void put(TableDesc td) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            TableDesc cur = getTable(td.getTableName());
            boolean update = (cur != null);
            if (update) {
                // TODO: check assumption/limitation that all columns in td are also in cur
                Set<String> curCols = new TreeSet<String>();
                for (ColumnDesc cd : cur.getColumnDescs()) {
                    log.debug("update: cur = " + cd.getColumnName());
                    curCols.add(cd.getColumnName());
                }
                Set<String> tdCols = new TreeSet<String>();
                for (ColumnDesc cd : td.getColumnDescs()) {
                    log.debug("update: td = " + cd.getColumnName());
                    tdCols.add(cd.getColumnName());
                }
                log.debug("update: " + curCols.size() + " vs " + tdCols.size());
                if (curCols.size() != tdCols.size()
                        || !curCols.containsAll(tdCols)
                        || !tdCols.containsAll(curCols)) {
                    throw new UnsupportedOperationException("cannot add/remove/rename columns");
                }
                
                for (ColumnDesc cd : cur.getColumnDescs()) {
                    ColumnDesc ncd = td.getColumn(cd.getColumnName());
                    if (!cd.getDatatype().equals(ncd.getDatatype())) {
                        throw new UnsupportedOperationException("cannot change column type: " + cd.getColumnName() 
                            + " " + cd.getDatatype() + " -> " + ncd.getDatatype());
                    }
                }
            }
            SchemaDesc sd = getSchema(td.getSchemaName());
            
            tm.startTransaction();
            
            if (sd == null) {
                sd = new SchemaDesc(td.getSchemaName());
                PutSchemaStatement pss = new PutSchemaStatement(update);
                log.debug("put missing schema: " + sd.getSchemaName());
                pss.setSchema(sd);
                jdbc.update(pss);
            }
            
            PutTableStatement pts = new PutTableStatement(update);
            log.debug("put: " + td.getTableName());
            pts.setTable(td);
            jdbc.update(pts);
            
            // add/remove columns not supported so udpate flag is same for the table and column(s)
            PutColumnStatement pcs = new PutColumnStatement(update);
            for (ColumnDesc cd : td.getColumnDescs()) {
                log.debug("put: " + cd.getColumnName());
                pcs.setColumn(cd);
                jdbc.update(pcs);
            }
            
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
                throw new RuntimeException("failed to persist " + td.getTableName(), ex);
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
    
    public void delete(String tableName) throws ResourceNotFoundException {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        try {
            TableDesc cur = getTable(tableName);
            if (cur == null) {
                throw new ResourceNotFoundException("not found: " + tableName);
            }
            
            tm.startTransaction();
            
            // delete all columns
            DeleteColumnsStatement dcs = new DeleteColumnsStatement();
            log.debug("delete columns: " + cur.getTableName());
            dcs.setTable(cur);
            jdbc.update(dcs);

            // delete table
            DeleteTableStatement dts = new DeleteTableStatement();
            log.debug("delete table: " + cur.getTableName());
            dts.setTable(cur);
            jdbc.update(dts);
            
            log.debug("commit transaction");
            tm.commitTransaction();
            log.debug("commit transaction: OK");
        } catch (ResourceNotFoundException rethrow) {
            throw rethrow;
        } catch (Exception ex) {
                try {
                    log.error("DELETE failed - rollback", ex);
                    tm.rollbackTransaction();
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
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    private String toCommaList(String[] strs, int numUpdateKeys) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<strs.length - numUpdateKeys; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(strs[i]);
        }
        return sb.toString();
    }
    
    private String toParamList(String[] strs, int numUpdateKeys) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<strs.length - numUpdateKeys; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
        return sb.toString();
    }
    
    private class GetSchemasStatement implements PreparedStatementCreator
    {
        private String tap_schema_tab;
        private String orderBy;

        public GetSchemasStatement(String tap_schema_tab)
        {
            this.tap_schema_tab = tap_schema_tab;
        }

        public void setOrderBy(String orderBy)
        {
            this.orderBy = orderBy;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsSchemaCols, 0));
            sb.append(" FROM ").append(tap_schema_tab);

            // customisation
            String tmp = appendWhere(tap_schema_tab, sb.toString());
            
            sb = new StringBuilder();
            sb.append(tmp);
            if (orderBy != null)
                sb.append(orderBy);
            
            String sql = sb.toString();
            log.debug(sql);

            PreparedStatement prep = conn.prepareStatement(sql);
            return prep;
        }
    }

    private class GetTablesStatement implements PreparedStatementCreator
    {
        private String tap_schema_tab;
        private String tableName;
        private String orderBy;

        public GetTablesStatement(String tap_schema_tab)
        {
            this.tap_schema_tab = tap_schema_tab;
        }

        public void setTableName(String tableName)
        {
            this.tableName = tableName;
        }

        public void setOrderBy(String orderBy)
        {
            this.orderBy = orderBy;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsTablesCols, 0));
            sb.append(" FROM ").append(tap_schema_tab);
            
            // customisation
            String tmp = appendWhere(tap_schema_tab, sb.toString());
            
            sb = new StringBuilder();
            sb.append(tmp);
            if (tableName != null)
            {
                if (tmp.toLowerCase().contains("where"))
                    sb.append(" AND ");
                else
                    sb.append(" WHERE ");
                sb.append(" table_name = ?");
            }
            if (orderBy != null)
                sb.append(orderBy);
            
            String sql = sb.toString();
            log.debug(sql);
            log.debug("values: " + tableName);
            
            PreparedStatement prep = conn.prepareStatement(sql);
            if (tableName != null)
                prep.setString(1, tableName);
            return prep;
        }
    }
    
    private class GetColumnsStatement implements PreparedStatementCreator
    {
        private String tap_schema_tab;
        private String tableName;
        private String orderBy;

        public GetColumnsStatement(String tap_schema_tab)
        {
            this.tap_schema_tab = tap_schema_tab;
        }

        public void setTableName(String tableName)
        {
            this.tableName = tableName;
        }

        public void setOrderBy(String orderBy)
        {
            this.orderBy = orderBy;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsColumnsCols, 0));
            sb.append(" FROM ").append(tap_schema_tab);
            
            // customisation
            String tmp = appendWhere(tap_schema_tab, sb.toString());
            
            sb = new StringBuilder();
            sb.append(tmp);
            if (tableName != null)
            {
                if (tmp.toLowerCase().contains("where"))
                    sb.append(" AND ");
                else
                    sb.append(" WHERE ");
                sb.append(" table_name = ?");
            }
            if (orderBy != null)
                sb.append(orderBy);
            
            String sql = sb.toString();
            log.debug(sql);
            log.debug("values: " + tableName);
            
            PreparedStatement prep = conn.prepareStatement(sql);
            if (tableName != null)
                prep.setString(1, tableName);
            return prep;
        }
    }
    
    private class GetKeysStatement implements PreparedStatementCreator
    {
        private String tap_schema_tab;
        private String tableName;
        private String orderBy;

        public GetKeysStatement(String tap_schema_tab)
        {
            this.tap_schema_tab = tap_schema_tab;
        }

        public void setTableName(String tableName)
        {
            this.tableName = tableName;
        }

        public void setOrderBy(String orderBy)
        {
            this.orderBy = orderBy;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsKeysCols, 0));
            sb.append(" FROM ").append(tap_schema_tab);
            
            // customisation
            String tmp = appendWhere(tap_schema_tab, sb.toString());
            
            sb = new StringBuilder();
            sb.append(tmp);
            if (tableName != null)
            {
                if (tmp.toLowerCase().contains("where"))
                    sb.append(" AND ");
                else
                    sb.append(" WHERE ");
                sb.append(" from_table = ?");
            }
            else if (orderBy != null)
                sb.append(orderBy);
            
            String sql = sb.toString();
            log.debug(sql);
            log.debug("values: " + tableName);
            
            PreparedStatement prep = conn.prepareStatement(sql);
            if (tableName != null)
                prep.setString(1, tableName);
            return prep;
        }
    }
    
    private class GetKeyColumnsStatement implements PreparedStatementCreator
    {
        private String tap_schema_tab;
        private List<KeyDesc> keyDescs;
        private String orderBy;

        public GetKeyColumnsStatement(String tap_schema_tab)
        {
            this.tap_schema_tab = tap_schema_tab;
        }

        public void setKeyDescs(List<KeyDesc> keyDescs)
        {
            this.keyDescs = keyDescs;
        }

        public void setOrderBy(String orderBy)
        {
            this.orderBy = orderBy;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(toCommaList(tsKeyColumnsCols, 0));
            sb.append(" FROM ").append(tap_schema_tab);
            
            // customisation
            String tmp = appendWhere(tap_schema_tab, sb.toString());
            
            sb = new StringBuilder();
            sb.append(tmp);
            if (keyDescs != null && !keyDescs.isEmpty())
            {
                if (tmp.toLowerCase().contains("where"))
                    sb.append(" AND ");
                else
                    sb.append(" WHERE ");
                sb.append("key_id IN (");
                for (KeyDesc kd : keyDescs)
                {
                    sb.append("?,");
                }
                sb.setCharAt(sb.length() - 1, ')'); // replace last | with closed bracket
            }
            else if (orderBy != null)
                sb.append(orderBy);
            
            String sql = sb.toString();
            log.debug(sql);
            
            PreparedStatement prep = conn.prepareStatement(sql);
            if (keyDescs != null && !keyDescs.isEmpty())
            {
                int col = 1;
                for (KeyDesc kd : keyDescs)
                {
                    log.debug("values: " + kd.getKeyID());
                    prep.setString(col++, kd.getKeyID());
                }
            }
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
            
            // load values: description, utype, schema_index, schema_name
            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, schema.description);
            safeSetString(sb, ps, col++, schema.utype);
            safeSetInteger(sb, ps, col++, schema.schema_index);
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
                sb.append(toCommaList(tsTablesCols, 2));
                sb.append(") = (");
                sb.append(toParamList(tsTablesCols, 2));
                sb.append(")");
                sb.append(" WHERE schema_name=? AND table_name=?");
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
            
            // load values: description, utype, schema_name, table_name
            sb = new StringBuilder();
            int col = 1;
            safeSetString(sb, ps, col++, table.tableType.getValue());
            safeSetString(sb, ps, col++, table.description);
            safeSetString(sb, ps, col++, table.utype);
            safeSetInteger(sb, ps, col++, table.tableIndex);
            safeSetString(sb, ps, col++, table.getSchemaName());
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
            
            // description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, id, 
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
            safeSetString(sb, ps, col++, column.id);
            safeSetInteger(sb, ps, col++, column.column_index);
            safeSetString(sb, ps, col++, column.getTableName());
            safeSetString(sb, ps, col++, column.getColumnName());
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
     * Append a where clause to the query that selects from the specified table.
     * The default implementation does nothing (returns in the provided SQL as-is).
     * 
     * 
     * If you want to implement some additional conditions, such as having private records
     * only visible to certain authenticated and authorized users, you can append some
     * conditions (or re-write the query as long as the select-list is not altered) here.
     * 
     * @param sql
     * @return modified SQL
     */
    protected String appendWhere(String tapSchemaTablename, String sql)
    {
        return sql;
    }

    /**
     * Creates Lists of Tables with a common Schema name, then adds the Lists to the Schemas.
     * 
     * @param schemaDescs List of Schemas.
     * @param tableDescs List of Tables.
     */
    private void addTablesToSchemas(List<SchemaDesc> schemaDescs, List<TableDesc> tableDescs)
    {
        for (TableDesc tableDesc : tableDescs)
        {
            for (SchemaDesc schemaDesc : schemaDescs)
            {
                if (tableDesc.getSchemaName().equals(schemaDesc.getSchemaName()))
                {
                    schemaDesc.getTableDescs().add(tableDesc);
                    break;
                }
            }
        }
    }

    /**
     * Creates Lists of Columns with a common Table name, then adds the Lists to the Tables.
     * 
     * @param tableDescs List of Tables.
     * @param columnDescs List of Columns.
     */
    private void addColumnsToTables(List<TableDesc> tableDescs, List<ColumnDesc> columnDescs)
    {
        for (ColumnDesc col : columnDescs)
        {
            for (TableDesc tableDesc : tableDescs)
            {
                if (col.getTableName().equals(tableDesc.getTableName()))
                {
                    tableDesc.getColumnDescs().add(col);
                    break;
                }
            }
        }
    }

    /**
     * Creates Lists of KeyColumns with a common Key keyID, then adds the Lists to the Keys.
     * 
     * @param keyDescs List of Keys.
     * @param keyColumnDescs List of KeyColumns.
     */
    private void addKeyColumnsToKeys(List<KeyDesc> keyDescs, List<KeyColumnDesc> keyColumnDescs)
    {
        for (KeyColumnDesc keyColumnDesc : keyColumnDescs)
        {
            for (KeyDesc keyDesc : keyDescs)
            {
                if (keyColumnDesc.getKeyID().equals(keyDesc.getKeyID()))
                {
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
    private void addForeignKeys(List<SchemaDesc> schemaDescs, List<KeyDesc> keyDescs)
    {
        for (KeyDesc key : keyDescs)
        {
            for (SchemaDesc sd : schemaDescs)
            {
                for (TableDesc td : sd.getTableDescs())
                {
                    if ( key.getFromTable().equals(td.getTableName()))
                    {
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
    protected List<FunctionDesc> getFunctionDescs()
    {
        List<FunctionDesc> functionDescs = new ArrayList<FunctionDesc>();

        // ADQL functions.
        functionDescs.add(new FunctionDesc("AREA", TapDataType.DOUBLE, "deg**2"));
        //functionDescs.add(new FunctionDesc("BOX", new TapDataType("adql:REGION", null, null, null)));
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
        //functionDescs.add(new FunctionDesc("REGION", new TapDataType("adql:REGION", null, null, null)));

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
         * Part of the ADQL BNF, but currently not parseable pending bug
         * fix in the jsqlparser.
         *
         * functionDescs.add(new FunctionDesc("PI", "", "adql:DOUBLE"));
         */
        functionDescs.add(new FunctionDesc("POWER", TapDataType.FUNCTION_ARG));
        functionDescs.add(new FunctionDesc("RADIANS", TapDataType.DOUBLE, "radians"));
        /*
         * Part of the ADQL BNF, but currently not parseable pending bug
         * fix in the jsqlparser.
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
//        functionDescs.add(new FunctionDesc("BIT_LENGTH", "", "adql:INTEGER"));
//        functionDescs.add(new FunctionDesc("CHARACTER_LENGTH", "", "adql:INTEGER"));
        functionDescs.add(new FunctionDesc("LOWER", new TapDataType("char", "*", null)));
//        functionDescs.add(new FunctionDesc("OCTET_LENGTH", "", "adql:INTEGER"));
//        functionDescs.add(new FunctionDesc("OVERLAY", "", "adql:VARCHAR")); //SQL92???
//        functionDescs.add(new FunctionDesc("POSITION", "", "adql:INTEGER"));
        functionDescs.add(new FunctionDesc("SUBSTRING", new TapDataType("char", "*", null)));
//        functionDescs.add(new FunctionDesc("TRIM", new TapDataType("char", "*", null)));
        functionDescs.add(new FunctionDesc("UPPER", new TapDataType("char", "*", null)));

        // SQL Date functions.
//        functionDescs.add(new FunctionDesc("CURRENT_DATE", "", "adql:TIMESTAMP"));
//        functionDescs.add(new FunctionDesc("CURRENT_TIME", "", "adql:TIMESTAMP"));
//        functionDescs.add(new FunctionDesc("CURRENT_TIMESTAMP", "", "adql:TIMESTAMP"));
//        functionDescs.add(new FunctionDesc("EXTRACT", "", "adql:TIMESTAMPs"));
//        functionDescs.add(new FunctionDesc("LOCAL_DATE", "", "adql:TIMESTAMP"));   //SQL92???
//        functionDescs.add(new FunctionDesc("LOCAL_TIME", "", "adql:TIMESTAMP"));   //SQL92???
//        functionDescs.add(new FunctionDesc("LOCAL_TIMESTAMP", "", "adql:TIMESTAMP"));  //SQL92???

        return functionDescs;
    }

    /**
     * Creates a List of Schema populated from the ResultSet.
     */
    private static final class SchemaMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException
        {
            String sn = rs.getString("schema_name");
            SchemaDesc schemaDesc = new SchemaDesc(sn);
            
            schemaDesc.description = rs.getString("description");
            schemaDesc.utype = rs.getString("utype");
            
            return schemaDesc;
        }
    }

    /**
     * Creates a List of Table populated from the ResultSet.
     */
    private static final class TableMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException
        {
            String sn = rs.getString("schema_name");
            String tn = rs.getString("table_name");
            TableDesc tableDesc = new TableDesc(sn, tn);
            
            tableDesc.tableType = TableDesc.TableType.toValue(rs.getString("table_type"));
            tableDesc.description = rs.getString("description");
            tableDesc.utype = rs.getString("utype");
            
            return tableDesc;
        }
    }

    /**
     * Creates a List of Column populated from the ResultSet.
     */
    private static final class ColumnMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException
        {
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
            col.id = rs.getString("id");
            
            return col;
        }

        private boolean intToBoolean(Integer i)
        {
            if (i == null)
                return false;
            return (i == 1);
        }
    }

    /**
     * Creates a List of Key populated from the ResultSet.
     */
    private static final class KeyMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException
        {
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
    private static final class KeyColumnMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException
        {
            String kid = rs.getString("key_id");
            String fc = rs.getString("from_column");
            String tc = rs.getString("target_column");
            KeyColumnDesc keyColumnDesc = new KeyColumnDesc(kid, fc, tc);

            return keyColumnDesc;
        }
    }

    protected void safeSetString(StringBuilder sb, PreparedStatement ps, int col, String val)
            throws SQLException {
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
    
    protected void safeSetInteger(StringBuilder sb, PreparedStatement ps, int col, Integer val)
            throws SQLException {
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

    protected void safeSetBoolean(StringBuilder sb, PreparedStatement ps, int col, Boolean val)
            throws SQLException {
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
