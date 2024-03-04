/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.tap.db.DatabaseDataType;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.db.TapConstants;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.JDOMVOTableParser;
import ca.nrc.cadc.tap.upload.UploadParameters;
import ca.nrc.cadc.tap.upload.UploadTable;
import ca.nrc.cadc.tap.upload.VOTableParser;
import ca.nrc.cadc.tap.upload.VOTableParserException;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.tap.io.TableDataInputStream;

/**
 *
 * @author jburke
 */
public class BasicUploadManager implements UploadManager
{
    private static final Logger log = Logger.getLogger(BasicUploadManager.class);
    
    // Number of rows to insert per commit.
    private static final int NUM_ROWS_PER_COMMIT = 100;
    
    /**
     * DataSource for the DB.
     */
    protected DataSource dataSource;

    /**
     * Database Specific data type.
     */
    protected DatabaseDataType databaseDataType;

    /**
     * IVOA DateFormat
     */
    protected DateFormat dateFormat;

    /**
     * Maximum number of rows allowed in the UPLOAD VOTable.
     */
    protected final int maxUploadRows;

    protected Job job;
    
    /**
     * Default constructor.
     */
    private BasicUploadManager() {
        this(Integer.MAX_VALUE);
    }
    
    protected BasicUploadManager(int maxUploadRows)
    {
        this.maxUploadRows = maxUploadRows;
        dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    }

    @Override
    public String getUploadSchema()
    {
        return "TAP_UPLOAD";
    }
    
    /**
     * Set the DataSource used for creating and populating tables.
     * @param ds
     */
    @Override
    public void setDataSource(DataSource ds)
    {
        this.dataSource = ds;
    }

    /**
     * Give database specific data type information.
     *
     * @param databaseDataType The DatabaseDataType implementation.
     */
    @Override
    public void setDatabaseDataType(DatabaseDataType databaseDataType) {
        this.databaseDataType = databaseDataType;
    }

    @Override
    public void setJob(Job job)
    {
        this.job = job;
    }

    /**
     * Find and process all UPLOAD requests.
     *
     * @param paramList list of all parameters passed to the service.
     * @param jobID the UWS jobID.
     * @return map of service generated upload table name to user-specified table metadata
     */
    @Override
    public Map<String, TableDesc> upload(List<Parameter> paramList, String jobID)
    {
        log.debug("upload jobID " + jobID);
        
        if (dataSource == null)
            throw new IllegalStateException("failed to get DataSource");

        // Map of database table name to table descriptions.
        Map<String, TableDesc> metadata = new HashMap<String, TableDesc>();

        UploadTable cur = null;

        //FormatterFactory factory = DefaultFormatterFactory.getFormatterFactory();
        //factory.setJobID(jobID);
        //factory.setParamList(params);

        try
        {
            // Get upload table names and URI's from the request parameters.
            UploadParameters uploadParameters = new UploadParameters(paramList, jobID);
            if (uploadParameters.uploadTables.isEmpty())
            {
                log.debug("No upload tables found in paramList");
                return metadata;
            }

            // Process each table.
            for (UploadTable uploadTable : uploadParameters.uploadTables)
            {
                cur = uploadTable;
                
                // XML parser
                // TODO: make configurable.
                log.debug(uploadTable);
                
                final VOTableParser parser = getVOTableParser(uploadTable);

                // Get the Table description.
                TableDesc tableDesc = parser.getTableDesc();
                sanitizeTable(tableDesc);

                // Fully qualified name of the table in the database.
                String databaseTableName = getDatabaseTableName(uploadTable);

                metadata.put(databaseTableName, tableDesc);
                log.debug("upload table: " + databaseTableName + " aka " + tableDesc);
                
                final String tableName = tableDesc.getTableName();
                tableDesc.setTableName(databaseTableName);
                
                TableCreator tc = new TableCreator(dataSource);
                tc.createTable(tableDesc);
                
                TableLoader tld = new TableLoader(dataSource, 1000);
                tld.load(tableDesc, new TableDataInputStream() {
                    @Override
                    public void close() {
                        //no-op: fully read already
                    }

                    @Override
                    public Iterator<List<Object>> iterator() {
                        return new Iterator<List<Object>>() {
                            final Iterator<List<Object>> wrapped = parser.iterator();
                            int rowsRead = 0;

                            @Override
                            public boolean hasNext() {
                                return wrapped.hasNext() && rowsRead < maxUploadRows;
                            }

                            @Override
                            public List<Object> next() {
                                final List<Object> next = wrapped.next();
                                rowsRead++;

                                return next;
                            }

                            @Override
                            public void remove() {
                                wrapped.remove();
                            }

                            @Override
                            public void forEachRemaining(Consumer<? super List<Object>> action) {
                                wrapped.forEachRemaining(action);
                            }
                        };
                    }

                    @Override
                    public TableDesc acceptTargetTableDesc(TableDesc td) {
                        return td;
                    }
                });
                
                tableDesc.setTableName(tableName);
            }
        }
        catch(VOTableParserException ex)
        {
            throw new RuntimeException("failed to parse table " + cur.tableName + " from " + cur.uri, ex);
        }
        catch(IOException ex)
        {
            throw new RuntimeException("failed to read table " + cur.tableName + " from " + cur.uri, ex);
        }
       
        return metadata;
    }
    
    protected VOTableParser getVOTableParser(UploadTable uploadTable)
            throws IOException
    {
        VOTableParser ret = new JDOMVOTableParser();
        ret.setUpload(uploadTable);
        return ret;
    }

    /**
     * Remove redundant metadata like TAP-1.0 xtypes for primitive columns.
     * 
     * @param td 
     */
    protected void sanitizeTable(TableDesc td)
    {
        for (ColumnDesc cd : td.getColumnDescs())
        {
            String xtype = cd.getDatatype().xtype;
            if (TapConstants.TAP10_TIMESTAMP.equals(xtype))
                cd.getDatatype().xtype = "timestamp"; // DALI-1.1
            
            if (oldXtypes.contains(xtype))
                cd.getDatatype().xtype = null;
        }
    }
    // TAP-1.0 xtypes that can just be dropped from ColumnDesc
    private final List<String> oldXtypes = Arrays.asList(
            TapConstants.TAP10_CHAR,  TapConstants.TAP10_VARCHAR,
            TapConstants.TAP10_DOUBLE, TapConstants.TAP10_REAL,
            TapConstants.TAP10_BIGINT, TapConstants.TAP10_INTEGER, TapConstants.TAP10_SMALLINT
    );
    
    /**
     * Create the SQL to grant select privileges for the UPLOAD table.
     * 
     * @param databaseTableName fully qualified table name.
     * @return 
     */
    protected String getGrantSelectTableSQL(String databaseTableName)
    {
        return null;
    }
    
    /**
     * Constructs the database table name from the schema, upload table name,
     * and the jobID.
     * 
     * @return the database table name.
     */
    public String getDatabaseTableName(UploadTable uploadTable)
    {
        StringBuilder sb = new StringBuilder();
        if (!uploadTable.tableName.toUpperCase().startsWith(SCHEMA))
            sb.append(SCHEMA).append(".");
        sb.append(uploadTable.tableName);
        sb.append("_");
        sb.append(uploadTable.jobID);
        return sb.toString();
    }
}
