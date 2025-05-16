
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

import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.tap.db.DatabaseDataType;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.db.TapConstants;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.JDOMVOTableParser;
import ca.nrc.cadc.tap.upload.UploadLimits;
import ca.nrc.cadc.tap.upload.UploadParameters;
import ca.nrc.cadc.tap.upload.UploadTable;
import ca.nrc.cadc.tap.upload.VOTableParser;
import ca.nrc.cadc.tap.upload.VOTableParserException;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.tap.io.TableDataInputStream;

/**
 *
 * @author jburke
 */
public class BasicUploadManager implements UploadManager {

    private static final Logger log = Logger.getLogger(BasicUploadManager.class);

    // TAP-1.0 xtypes that can just be dropped from ColumnDesc
    private static final List<String> TAP10_XTYPES = Arrays.asList(
            TapConstants.TAP10_CHAR, TapConstants.TAP10_VARCHAR,
            TapConstants.TAP10_DOUBLE, TapConstants.TAP10_REAL,
            TapConstants.TAP10_BIGINT, TapConstants.TAP10_INTEGER, TapConstants.TAP10_SMALLINT
    );

    /**
     * DataSource for the DB.
     */
    protected DataSource dataSource;

    /**
     * IVOA DateFormat
     */
    protected DateFormat dateFormat;

    /**
     * Limitations on the UPLOAD VOTable.
     */
    protected final UploadLimits uploadLimits;

    protected Job job;

    /**
     * Backwards compatible constructor. This uses the default byte limit of 10MiB.
     *
     * @param rowLimit maximum number of rows
     * @deprecated use UploadLimits instead
     */
    @Deprecated
    protected BasicUploadManager(int rowLimit) {
        // 10MiB of votable xml is roughly 17k rows x 10 columns
        this(new UploadLimits(10 * 1024L * 1024L));
        this.uploadLimits.rowLimit = rowLimit;
    }

    /**
     * Subclass constructor.
     *
     * @param uploadLimits limits on table upload
     */
    protected BasicUploadManager(UploadLimits uploadLimits) {
        if (uploadLimits == null) {
            throw new IllegalStateException("Upload limits are required.");
        }

        this.uploadLimits = uploadLimits;
        dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    }

    @Override
    public String getUploadSchema() {
        return "TAP_UPLOAD";
    }

    /**
     * Set the DataSource used for creating and populating tables.
     *
     * @param ds
     */
    @Override
    public void setDataSource(DataSource ds) {
        this.dataSource = ds;
    }

    @Override
    public void setJob(Job job) {
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
    public Map<String, TableDesc> upload(List<Parameter> paramList, String jobID) {
        log.debug("upload jobID " + jobID);

        if (dataSource == null) {
            throw new IllegalStateException("failed to get DataSource");
        }

        // return a map of database table name to table descriptions.
        final Map<String, TableDesc> metadata = new HashMap<>();

        UploadTable cur = null;

        try {
            // Get upload table names and URI's from the request parameters.
            UploadParameters uploadParameters = new UploadParameters(paramList, jobID);
            if (uploadParameters.uploadTables.isEmpty()) {
                log.debug("No upload tables found in paramList");
                return metadata;
            }

            // Process each table.
            for (UploadTable uploadTable : uploadParameters.uploadTables) {
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
                try {
                    tableDesc.setTableName(databaseTableName);
                    storeTable(tableDesc, parser.getVOTable());
                } finally {
                    tableDesc.setTableName(tableName);
                }
            }
        } catch (VOTableParserException ex) {
            // user error
            throw new IllegalArgumentException("failed to parse uploaded table " + cur, ex);
        } catch (IOException ex) {
            // unclear if the is user or server error
            throw new RuntimeException("failed to read uploaded table " + cur, ex);
        }

        return metadata;
    }
    
    /**
     * Use TableCreator and TableLoader to create and load the table using the dataSource.
     * 
     * @param table the table description
     * @param vot the table data
     */
    protected void storeTable(TableDesc table, VOTableTable vot) {
        TableCreator tc = new TableCreator(dataSource);
        tc.createTable(table);
        
        // TODO: drop table if load fails? or leave for diagnostics
        TableLoader tld = new TableLoader(dataSource, 1000);
        tld.load(table, new TableDataInputStream() {
            @Override
            public void close() {
                //no-op: fully read already
            }

            @Override
            public Iterator<List<Object>> iterator() {
                return vot.getTableData().iterator();
            }

            @Override
            public TableDesc acceptTargetTableDesc(TableDesc td) {
                return td;
            }
        });
    }
    
    protected VOTableParser getVOTableParser(UploadTable uploadTable)
            throws VOTableParserException {
        VOTableParser ret = new JDOMVOTableParser(uploadLimits);
        ret.setUpload(uploadTable);
        return ret;
    }

    /**
     * Remove redundant metadata like TAP-1.0 xtypes for primitive columns.
     *
     * @param td
     */
    protected void sanitizeTable(TableDesc td) {
        for (ColumnDesc cd : td.getColumnDescs()) {
            String xtype = cd.getDatatype().xtype;
            if (TapConstants.TAP10_TIMESTAMP.equals(xtype)) {
                cd.getDatatype().xtype = "timestamp"; // DALI-1.1
            }
            if (TAP10_XTYPES.contains(xtype)) {
                cd.getDatatype().xtype = null;
            }
        }
    }

    /**
     * Create the SQL to grant select privileges for the UPLOAD table.
     *
     * @param databaseTableName fully qualified table name.
     * @return
     */
    protected String getGrantSelectTableSQL(String databaseTableName) {
        return null;
    }

    /**
     * Constructs the database table name from the schema, upload table name,
     * and the jobID.
     *
     * @return the database table name.
     */
    public String getDatabaseTableName(UploadTable uploadTable) {
        StringBuilder sb = new StringBuilder();
        if (!uploadTable.tableName.toUpperCase().startsWith(SCHEMA)) {
            sb.append(SCHEMA).append(".");
        }
        sb.append(uploadTable.tableName);
        sb.append("_");
        sb.append(uploadTable.jobID);
        return sb.toString();
    }
}
