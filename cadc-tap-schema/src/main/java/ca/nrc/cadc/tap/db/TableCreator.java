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

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.schema.ADQLIdentifierException;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Utility to create a database table based on tap_schema descriptor.
 * 
 * @author pdowler
 */
public class TableCreator {
    private static final Logger log = Logger.getLogger(TableCreator.class);

    private final DataSource dataSource;
    private DatabaseDataType ddType;
    
    public TableCreator(DataSource dataSource) { 
        this.dataSource = dataSource;
        PluginFactory pf = new PluginFactory();
        this.ddType = pf.getDatabaseDataType();
        log.debug("loaded: " + ddType.getClass().getName());
    }

    public void createTable(TableDesc table) {
        try {
            TapSchemaUtil.checkValidTableName(table.getTableName());
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid table name: " + table.getTableName(), ex);
        }
        try {
            for (ColumnDesc cd : table.getColumnDescs()) {
                TapSchemaUtil.checkValidIdentifier(cd.getColumnName());
            }
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
        Profiler prof = new Profiler(TableCreator.class);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            String sql = generateCreate(table);
            
            log.debug("sql:\n" + sql);
            jdbc.execute(sql);
            prof.checkpoint("create-table");
            
            // grant permissions
            sql = "GRANT select on " + table.getTableName() +  " to public";
            log.debug("sql:\n" + sql);
            jdbc.execute(sql);
            prof.checkpoint("grant-permissions");
            
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (Exception ex) {
            try {
                log.error("create table failed - rollback", ex);
                tm.rollbackTransaction();
                prof.checkpoint("rollback-transaction");
                log.error("create table failed - rollback: OK");
            } catch (Exception oops) {
                log.error("create table failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to create table " + table.getTableName(), ex);
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
     * Drop a table. This is implemented as a TRUNCATE followed by a DROP so that
     * space is immediately reclaimed and usable for other content.
     * 
     * @param tableName
     * @throws ResourceNotFoundException if the table does not exist
     */
    public void dropTable(String tableName) throws ResourceNotFoundException {
        try {
            TapSchemaUtil.checkValidTableName(tableName);
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid table name: " + tableName, ex);
        }
        
        Profiler prof = new Profiler(TableCreator.class);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            // truncate before drop so space can be reclaimed immediately
            String truncate = "TRUNCATE TABLE " + tableName;
            log.debug("sql:\n" + truncate);
            jdbc.execute(truncate);
            prof.checkpoint("truncate-table");
            
            String drop = "DROP TABLE " + tableName;
            log.debug("sql:\n" + drop);
            jdbc.execute(drop);
            prof.checkpoint("drop-table");

            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (Exception ex) {
            // TODO: categorise failures better
            if (ex.getMessage().contains("does not exist")) {
                // handled: log at debug level
                try {
                    log.debug("drop table failed - rollback", ex);
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                    log.debug("drop table failed - rollback: OK");
                } catch (Exception oops) {
                    log.error("drop table failed - rollback : FAIL", oops);
                }
                throw new ResourceNotFoundException("not found: " + tableName);
            } else {
                // unexpected: log at error level
                try {
                    log.error("drop table failed - rollback", ex);
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                    log.error("drop table failed - rollback: OK");
                } catch (Exception oops) {
                    log.error("drop table failed - rollback : FAIL", oops);
                }
            }
            throw new RuntimeException("failed to drop table " + tableName, ex);
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
    
    public void createIndex(ColumnDesc cd, boolean unique) {
        try {
            TapSchemaUtil.checkValidTableName(cd.getTableName());
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid table name: " + cd.getTableName(), ex);
        }
        try {
            TapSchemaUtil.checkValidIdentifier(cd.getColumnName());
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid column name: " + cd.getColumnName(), ex);
        }
        
        String sql = generateCreateIndex(cd, unique);
        
        Profiler prof = new Profiler(TableCreator.class);
        DatabaseTransactionManager tm = new DatabaseTransactionManager(dataSource);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        try {
            tm.startTransaction();
            prof.checkpoint("start-transaction");
            
            log.debug("sql:\n" + sql);
            jdbc.execute(sql);
            prof.checkpoint("create-index");
            
            tm.commitTransaction();
            prof.checkpoint("commit-transaction");
        } catch (Exception ex) {
            try {
                log.error("create index failed - rollback", ex);
                tm.rollbackTransaction();
                prof.checkpoint("rollback-transaction");
                log.error("create index failed - rollback: OK");
            } catch (Exception oops) {
                log.error("create index failed - rollback : FAIL", oops);
            }
            if (ex instanceof IllegalArgumentException) {
                throw ex;
            }
            throw new RuntimeException("failed to create index on " + cd.getTableName() + "(" + cd.getColumnName() + ")", ex);
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
    
    
    private String generateCreate(TableDesc td) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(td.getTableName()).append("(");
        for (int i = 0; i < td.getColumnDescs().size(); i++) {
            ColumnDesc columnDesc = td.getColumnDescs().get(i);
            sb.append(columnDesc.getColumnName());
            sb.append(" ");
            sb.append(ddType.getDataType(columnDesc));
            sb.append(" null ");
            if (i + 1 < td.getColumnDescs().size()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
    private String generateCreateIndex(ColumnDesc cd, boolean unique) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (unique) {
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX ");
        String indexName = "i_" + cd.getTableName().replace(".", "_") + "_" + cd.getColumnName();
        sb.append(indexName);
        sb.append(" ON ").append(cd.getTableName());
        
        String using = ddType.getIndexUsingQualifier(cd, unique);
        if (using != null) {
            sb.append(" USING ").append(using);
        }
        sb.append(" (");
        sb.append(cd.getColumnName());
        String iop = ddType.getIndexColumnOperator(cd);
        if (iop != null) {
            sb.append(" ").append(iop);
        }
        sb.append(")");
        
        return sb.toString();
    }
}
