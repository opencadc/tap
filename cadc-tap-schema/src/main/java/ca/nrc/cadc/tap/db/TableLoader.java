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

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.LongInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;

import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.tap.io.InconsistentTableDataException;
import org.opencadc.tap.io.TableDataInputStream;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

/**
 * Utility to bulk load content into a table.
 * 
 * @author pdowler, majorb
 */
public class TableLoader {
    private static final Logger log = Logger.getLogger(TableLoader.class);

    private final DatabaseDataType ddType;
    private final DataSource dataSource;
    public Integer batchSize;
    private long totalInserts = 0;
    
    /**
     * Constructor.
     * 
     * @param dataSource destination database connection pool
     * @param batchSize number of rows per commit transaction
     */
    public TableLoader(DataSource dataSource, Integer batchSize) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        PluginFactory pf = new PluginFactory();
        this.ddType = pf.getDatabaseDataType();
        log.debug("loaded: " + ddType.getClass().getName());
    }
    
    /**
     * Load the table data.
     * 
     * @param destTable The table description
     * @param data The table data.
     */
    public void load(TableDesc destTable, TableDataInputStream data) throws IOException {
        TableDesc reorgTable = data.acceptTargetTableDesc(destTable);

        boolean manageTxn = true;
        if (batchSize == null || batchSize <= 0) {
            manageTxn = false;
            batchSize = Integer.MAX_VALUE; // no batching, just one transaction
        }

        Profiler prof = new Profiler(TableLoader.class);
        DatabaseTransactionManager tm = manageTxn ? new DatabaseTransactionManager(dataSource) : null;
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        
        // Loop over rows, start/commit txn every batchSize rows
        String sql = generateInsertSQL(reorgTable); 
        boolean done = false;
        ResourceIterator<List<Object>> dataIterator = data.iterator();
        List<Object> nextRow = null;

        List<List<Object>> batch = manageTxn ? new ArrayList<>(batchSize) : new ArrayList<>();
        int count = 0;
        try {
            while (!done) {
                count = 0;
                if (manageTxn) {
                    tm.startTransaction();
                    prof.checkpoint("start-transaction");
                }
                BulkInsertStatement bulkInsertStatement = new BulkInsertStatement(reorgTable);
                
                while (batch.size() < batchSize && dataIterator.hasNext()) {
                    nextRow = dataIterator.next();
                    convertValueObjects(nextRow);
                    batch.add(nextRow);
                    count++;
                }
                log.debug("Inserting " + batch.size() + " rows in this batch.");
                jdbc.batchUpdate(sql, batch, batchSize, bulkInsertStatement);
                prof.checkpoint("batch-of-inserts");

                if (manageTxn) {
                    tm.commitTransaction();
                    prof.checkpoint("commit-transaction");
                }
                totalInserts += batch.size();
                batch.clear();
                done = !dataIterator.hasNext();
            }
        } catch (IllegalArgumentException | IndexOutOfBoundsException | InconsistentTableDataException ex) {
            try {
                data.close();
                prof.checkpoint("close-input");
            } catch (Exception oops) {
                log.error("unexpected exception trying to close input stream", oops);
            }
            try {
                if (manageTxn && tm.isOpen()) {
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                }
            } catch (Exception oops) {
                log.error("Unexpected: could not rollback transaction", oops);
            }
            throw new IllegalArgumentException("Inserted " + totalInserts + " rows." 
                    + " Current batch failed with: " + ex.getMessage() + " on line " + (totalInserts + count));
        } catch (Throwable t) {
            try {
                data.close();
                prof.checkpoint("close-input");
            } catch (Exception oops) {
                log.error("unexpected exception trying to close input stream", oops);
            }
            try {
                if (manageTxn && tm.isOpen()) {
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                }
            } catch (Throwable oops) {
                log.error("Unexpected: could not rollback transaction", oops);
            }

            log.debug("Batch insert failure", t);
            throw new RuntimeException("Inserted " + totalInserts + " rows."
                + " Current batch of " + batchSize + " failed with: " + t.getMessage(), t);
            
        } finally {
            if (manageTxn && tm.isOpen()) {
                log.error("BUG: Transaction manager unexpectedly open, rolling back.");
                try {
                    tm.rollbackTransaction();
                    prof.checkpoint("rollback-transaction");
                } catch (Throwable t) {
                    log.error("Unexpected: could not rollback transaction", t);
                }
            }
            try {
                data.close();
            } catch (Exception ex) {
                log.debug("exception trying to close input stream in finally: ignoring it", ex);
            }
        }
        log.debug("Inserted a total of " + totalInserts + " rows.");
    }
    
    // this assumes that columns in destTable and data are in the same order
    // generate a parameterized insert statement for use with one of the API choices
    private String generateInsertSQL(TableDesc td) {
        
        StringBuilder sb = new StringBuilder("insert into ");
        sb.append(td.getTableName());
        sb.append(" (");
        for (ColumnDesc cd : td.getColumnDescs()) {
            sb.append(cd.getColumnName());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(") values (");
        for (ColumnDesc cd : td.getColumnDescs()) {
            sb.append("?, ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        return sb.toString();
    }
    
    private class BulkInsertStatement implements ParameterizedPreparedStatementSetter<List<Object>> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private TableDesc tableDesc;
        
        public BulkInsertStatement(TableDesc tableDesc) {
            this.tableDesc = tableDesc;
        }

        @Override
        public void setValues(PreparedStatement ps, List<Object> row) throws SQLException {
            int col = 1;
            for (Object val : row) {
                ColumnDesc cd = tableDesc.getColumnDescs().get(col - 1);
                if (val != null && val instanceof Date && TapDataType.TIMESTAMP.equals(cd.getDatatype())) {
                    Date d = (Date) val;
                    ps.setTimestamp(col++, new Timestamp(d.getTime()), utc);
                } else {
                    ps.setObject(col++, val);
                }
            }
        }
    }
    
    /**
     * @return The total number of rows inserted.
     */
    public long getTotalInserts() {
        return totalInserts;
    }
    
    // convert values that the JDBC driver won't accept
    private void convertValueObjects(List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            Object v = values.get(i);
            if (v != null) {
                Object nv = convertValueObject(v);
                if (v != nv) {
                    values.set(i, nv);
                }
            }
        }
    }
    
    private Object convertValueObject(Object v) {
        if (v instanceof URI) {
            return ((URI) v).toASCIIString();
        }
        if (v instanceof DoubleInterval) {
            return ddType.getIntervalObject((DoubleInterval) v);
        }
        if (v instanceof LongInterval) {
            Interval inter = (Interval) v;
            DoubleInterval di = new DoubleInterval(inter.getLower().doubleValue(), inter.getUpper().doubleValue());
            return ddType.getIntervalObject(di);
        }
        if (v instanceof Point) {
            return ddType.getPointObject((Point) v);
        }
        if (v instanceof Circle) {
            return ddType.getCircleObject((Circle) v);
        }
        if (v instanceof Polygon) {
            return ddType.getPolygonObject((Polygon) v);
        }
        if (v instanceof ca.nrc.cadc.stc.Position) {
            return ddType.getPointObject((ca.nrc.cadc.stc.Position) v);
        }
        if (v instanceof ca.nrc.cadc.stc.Region) {
            return ddType.getRegionObject((ca.nrc.cadc.stc.Region) v);
        }
        if (v instanceof short[]) {
            return ddType.getArrayObject((short[]) v);
        }
        if (v instanceof int[]) {
            return ddType.getArrayObject((int[]) v);
        }
        if (v instanceof long[]) {
            return ddType.getArrayObject((long[]) v);
        }
        if (v instanceof float[]) {
            return ddType.getArrayObject((float[]) v);
        }
        if (v instanceof double[]) {
            return ddType.getArrayObject((double[]) v);
        }
        
        return v;
    }
}
