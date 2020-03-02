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
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * Class to produce formatted row and column data through an iterator.
 * 
 * @author majorb
 *
 */
public class AsciiTableData implements TableDataInputStream, Iterator<List<Object>> {
    
    private static final Logger log = Logger.getLogger(AsciiTableData.class);
    
    private final CSVParser reader;
    private final Iterator<CSVRecord> rowIterator;
    private List<String> columnNames;
    private List<Format> columnFormats;
    
    /**
     * Constructor.
     * 
     * @param in The data stream
     * @param contentType The content type of the data
     * @throws IOException If a data handling error occurs
     */
    public AsciiTableData(InputStream in, String contentType) throws IOException {
        char delimiter = ',';
        if (contentType.equals(TableContentHandler.CONTENT_TYPE_TSV)) {
            delimiter = '\t';
        }
        InputStreamReader ir = new InputStreamReader(in);
        
        if (TableContentHandler.CONTENT_TYPE_TSV.equals(contentType)) {
            this.reader = new CSVParser(ir, CSVFormat.TDF.withFirstRecordAsHeader());
        } else if (TableContentHandler.CONTENT_TYPE_CSV.equals(contentType)) {
            this.reader = new CSVParser(ir, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        } else {
            throw new UnsupportedOperationException("contentType: " + contentType);
        }
        
        this.rowIterator = reader.iterator();
        Map<String,Integer> header = reader.getHeaderMap();
        columnNames = new ArrayList<String>(header.size());
        for (String s : header.keySet()) {
            columnNames.add(s.trim());
            log.debug("found column: " + s);
        }
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("No data columns.");
        }
    }

    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException ex) {
                log.debug("failed to close CSVParser", ex);
            }
        }
    }
    
    /**
     * Return the data iterator.
     * @return 
     */
    @Override
    public Iterator<List<Object>> iterator() {
        return this;
    }

    /**
     * @return True if the data stream has more rows.
     */
    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    /**
     * @return The list of formatted objects representing a row of data.
     */
    @Override
    public List<Object> next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more data to read.");
        }
        
        CSVRecord rec = rowIterator.next();
        if (rec.size() != columnNames.size()) {
            throw new IllegalArgumentException("wrong number of columns (" 
                    + rec.size() + ") expected " + columnNames.size());
        }
        try {
            List<Object> row = new ArrayList<Object>(columnNames.size());
            String cell = null;
            Object value = null;
            Format format = null;
            for (int i = 0; i < rec.size(); i++) {
                cell = rec.get(i);
                format = columnFormats.get(i);
                value = format.parse(cell);
                row.add(value);
            }
            return row;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid number: " + ex.getMessage());
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Set column formatters for parsing. This is an alternative to calling
     * acceptTargetTableDesc.
     * 
     * @param columnFormats 
     */
    public void setColumnFormats(List<Format> columnFormats) {
        this.columnFormats = columnFormats;
    }
    
    @Override
    public TableDesc acceptTargetTableDesc(TableDesc target) {
        TableDesc td = new TableDesc(target.getSchemaName(), target.getTableName());
        ColumnDesc colDesc = null;
        for (String col : columnNames) {
            colDesc = target.getColumn(col);
            if (colDesc == null) {
                throw new IllegalArgumentException("Unrecognized column name: " + col);
            }
            td.getColumnDescs().add(colDesc);
        }        
        createColumnFormats(td);
        return td;
    }
    
    private void createColumnFormats(TableDesc tableDesc) {
        FormatFactory formatFactory = new FormatFactory();
        this.columnFormats = new ArrayList<Format>(columnNames.size());
        for (String col : columnNames) {
            ColumnDesc colDesc = tableDesc.getColumn(col);
            VOTableField voTableField = TapSchemaUtil.convert(colDesc);
            Format format = formatFactory.getFormat(voTableField);
            columnFormats.add(format);
        }
    }

}
