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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;

import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import ca.nrc.cadc.vosi.actions.TableContentHandler;

/**
 * Class to produce formatted row and column data through an iterator.
 * 
 * @author majorb
 *
 */
public class AsciiTableData implements TableData, Iterator<List<Object>> {
    
    private static final Logger log = Logger.getLogger(AsciiTableData.class);
    
    private TableDesc tableDesc;
    private CsvReader reader;
    private List<String> columnNames;
    private Map<String, Format> columnFormats;
    private boolean hasNext;
    private FormatFactory formatFactory = new FormatFactory();
    
    /**
     * Constructor.
     * 
     * @param in The data stream
     * @param contentType The content type of the data
     * @param orig The original table description
     * @throws IOException If a data handling error occurs
     */
    public AsciiTableData(InputStream in, String contentType, TableDesc orig) throws IOException {
        char delimiter = ',';
        if (contentType.equals(TableContentHandler.CONTENT_TYPE_TSV)) {
            delimiter = '\t';
        }
        reader = new CsvReader(in, delimiter, Charset.defaultCharset());
        if (!reader.readHeaders()) {
            throw new RuntimeException("No inline data.");
        }
        columnNames = Arrays.asList(reader.getHeaders());
        tableDesc = createTableDesc(orig);
        columnFormats = createColumnFormats();
        hasNext = reader.readRecord();
    }

    /**
     * Return the data iterator.
     */
    @Override
    public Iterator<List<Object>> iterator() {
        return this;
    }
    
    /**
     * Get the table description for the data stream.
     * @return
     */
    public TableDesc getTableDesc() {
        return tableDesc;
    }

    /**
     * @return True if the data stream has more rows.
     */
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * @return The list of formatted objects representing a row of data.
     */
    @Override
    public List<Object> next() {
        if (!hasNext) {
            throw new IllegalStateException("No more data to read.");
        }
        if (reader.getColumnCount() != columnNames.size()) {
            throw new IllegalArgumentException("wrong number of columns (" +
                reader.getColumnCount() + ") expected " + columnNames.size());
        }
        try {
            List<Object> row = new ArrayList<Object>(columnNames.size());
            String cell = null;
            Object value = null;
            Format format = null;
            for (String col : columnNames) {
                cell = reader.get(col);
                format = columnFormats.get(col);
                value = format.parse(cell);
                row.add(value);
            }
            hasNext = reader.readRecord();
            return row;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data stream.", e);
        }
    }
    
    private TableDesc createTableDesc(TableDesc orig) {
        if (columnNames.size() == 0) {
            throw new IllegalArgumentException("No data columns.");
        }
        TableDesc tableDesc = new TableDesc(orig.getSchemaName(), orig.getTableName());
        ColumnDesc colDesc = null;
        for (String col : columnNames) {
            colDesc = orig.getColumn(col);
            if (colDesc == null) {
                throw new IllegalArgumentException("Unrecognized column name: " + col);
            }
            tableDesc.getColumnDescs().add(colDesc);
        }
        return tableDesc;
    }
    
    private Map<String, Format> createColumnFormats() {
        columnFormats = new HashMap<String, Format>(columnNames.size());
        for (String col : columnNames) {
            ColumnDesc colDesc = tableDesc.getColumn(col);
            VOTableField voTableField = TapSchemaUtil.convert(colDesc);
            Format format = formatFactory.getFormat(voTableField);
            columnFormats.put(col, format);
        }
        return columnFormats;
    }

}
