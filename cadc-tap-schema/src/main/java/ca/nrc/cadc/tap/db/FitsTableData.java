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

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
import org.opencadc.tap.io.TableDataInputStream;
import uk.ac.starlink.fits.FitsTableBuilder;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.TableSink;

public class FitsTableData implements TableDataInputStream {

    private static final int QUEUE_BUFFER_SIZE = 10000;

    private static final Logger log = Logger.getLogger(FitsTableData.class);

    private Map<String, Format<?>> columnFormats;
    private FormatFactory formatFactory = new FormatFactory();
    private FitsRowIterator iterator;
    private FitsTableReader sink;
    private List<String> columnNames;
    private int colCount;

    public FitsTableData(final InputStream in) throws IOException {
        try {

            BlockingQueue<Object[]> queue = new ArrayBlockingQueue<Object[]>(QUEUE_BUFFER_SIZE);
            sink = new FitsTableReader(queue);
            iterator = new FitsRowIterator(queue);

            // launch a thread to read the table data into a queue
            Runnable r = new Runnable() {
                public void run() {
                    try {
                        new FitsTableBuilder().streamStarTable(in, sink, null);
                    } catch (Throwable t) {
                        sink.throwable = t;
                        throw new RuntimeException("Queue producing thread failed", t);
                    }
                }
            };
            Thread t = new Thread(r, "FitsQueueDataProducer");
            t.start();

            // wait for the metadata (headers) to be read
            log.debug("meta done: " + sink.isMetaDone());
            log.debug("sink throwable: " + sink.getThrowable());
            while (!sink.isMetaDone() && sink.getThrowable() == null) {
                try {
                    log.debug("sleeping for 10ms");
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.debug("caught: " + e);
                }
                log.debug("meta done: " + sink.isMetaDone());
                log.debug("sink throwable: " + sink.getThrowable());
            }

            if (sink.getThrowable() != null) {
                throw new RuntimeException("Metadata reading failed: " + sink.getThrowable());
            }

        } catch (Exception e) {
            log.debug("Error reading fits file", e);
            throw new IllegalArgumentException("Error reading fits file: " + e.getMessage());
        }
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return iterator;
    }

    @Override
    public void close() {
        try {
            iterator.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close FITS stream.", e);
        }
    }

    @Override
    public TableDesc acceptTargetTableDesc(TableDesc target) {
        TableDesc td = new TableDesc(target.getSchemaName(), target.getTableName());

        List<ColumnInfo> cols = sink.getColumns();
        log.debug("Column count: " + cols.size());
        if (cols.size() < 1) {
            throw new IllegalArgumentException("No data columns");
        }
        columnNames = new ArrayList<String>(cols.size());
        ColumnDesc colDesc = null;
        String colName = null;
        for (ColumnInfo col : cols) {
            colName = col.getName();
            columnNames.add(colName);
            colDesc = target.getColumn(colName);
            if (colDesc == null) {
                throw new IllegalArgumentException("Unrecognized column name: " + colName);
            }
            td.getColumnDescs().add(colDesc);
        }
        columnFormats = createColumnFormats(td);
        colCount = columnFormats.size();
        return td;
    }

    private Map<String, Format<?>> createColumnFormats(TableDesc tableDesc) {
        Map<String, Format<?>> cf = new HashMap<String, Format<?>>(tableDesc.getColumnDescs().size());
        for (ColumnDesc colDesc : tableDesc.getColumnDescs()) {
            VOTableField voTableField = TapSchemaUtil.convert(colDesc);
            Format<?> format = formatFactory.getFormat(voTableField);
            log.debug("Created format: " + format);
            cf.put(colDesc.getColumnName(), format);
        }
        return cf;
    }

    class FitsRowIterator implements Iterator<List<Object>> {

        BlockingQueue<Object[]> queue;

        FitsRowIterator(BlockingQueue<Object[]> queue) {
            this.queue = queue;
        }

        public void close() throws IOException {
            // nothing to close
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (sink.getThrowable() != null) {
                log.debug("Producing thread throwable detected", sink.getThrowable());
                throw new RuntimeException("Producing thread throwable", sink.getThrowable());
            }
            return !queue.isEmpty() || !sink.isDone();
        }

        @Override
        public List<Object> next() {
            if (!this.hasNext()) {
                throw new IllegalStateException("No more data to read.");
            }

            Object[] nextRow = null;
            try {
                nextRow = queue.take();
                log.debug("Took row from queue: " + Arrays.toString(nextRow));
            } catch (InterruptedException e) {
                throw new IllegalStateException("Interrupeted while taking queue rows", e);
            }
            if (nextRow.length != colCount) {
                throw new IllegalArgumentException("wrong number of columns ("
                        + nextRow.length + ") expected " + colCount);
            }

            List<Object> ret = new ArrayList<Object>(colCount);
            Object cell = null;
            Object value = null;
            Format<?> format = null;
            String colName = null;
            for (int i = 0; i < colCount; i++) {
                colName = columnNames.get(i);
                format = columnFormats.get(colName);
                cell = nextRow[i];
                value = null;
                String strValue = convert(cell);
                if (strValue != null) {
                    value = format.parse(strValue);
                }
                ret.add(value);
            }
            return ret;

        }

        /**
         * Currently conversion is simply turning the object
         * back into the string form that the formaters expect
         * on the parse() method.
         *
         * @param o
         * @return The object in an expected string format.
         */
        private String convert(Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof String) {
                return (String) o;
            }
            if (o.getClass().isArray()) {
                int length = Array.getLength(o);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length; i++) {
                    sb.append(Array.get(o, i));
                    sb.append(" ");
                }
                if (sb.length() > 0) {
                    sb.setLength(sb.length() - 1);
                }
                return sb.toString();
            }
            return o.toString();
        }

    }

    class FitsTableReader implements TableSink {

        StarTable meta;
        BlockingQueue<Object[]> queue;
        boolean done = false;
        boolean metaDone = false;
        Throwable throwable = null;

        public FitsTableReader(BlockingQueue<Object[]> queue) {
            this.queue = queue;
        }

        public void acceptMetadata(StarTable meta) {
            this.meta = meta;
            metaDone = true;
        }

        public List<ColumnInfo> getColumns() {
            if (meta == null) {
                IllegalStateException e = new IllegalStateException("BUG: stream hasn't been started.");
                throwable = e;
                throw e;
            }

            List<ColumnInfo> cols = new ArrayList<ColumnInfo>(meta.getColumnCount());
            ColumnInfo next;
            for (int i = 0; i < meta.getColumnCount(); i++) {
                next = meta.getColumnInfo(i);
                cols.add(next);
                log.debug("Added column: " + next);
            }

            return cols;
        }

        public void acceptRow(Object[] row) {
            try {
                queue.put(row);
                log.debug("Put row in queue: " + Arrays.toString(row));
            } catch (InterruptedException e) {
                throwable = e;
                throw new IllegalStateException("Interrupeted while inserting queue rows", e);
            }
        }

        public void endRows() {
            log.debug("endRows called");
            done = true;
        }

        public boolean isMetaDone() {
            return metaDone;
        }

        public boolean isDone() {
            return done;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }
}
