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
 *  : 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Utility to read a table description from the database and generate a TableDesc 
 * that could be used to validate or add to the tap_schema.
 * 
 * @author pdowler
 */
public class TableIngester {
    private static final Logger log = Logger.getLogger(TableIngester.class);

    private final DataSource dataSource;
    private final DatabaseDataType databaseDataType;

    public TableIngester(DataSource dataSource) {
        this.dataSource = dataSource;
        PluginFactory pluginFactory = new PluginFactory();
        this.databaseDataType = pluginFactory.getDatabaseDataType();
        log.debug("loaded: " + databaseDataType.getClass().getName());
    }

    /**
     * Read the table description of a table in the database.
     * @param schemaName
     * @param tableName
     * @return the TableDesc with default permissions
     * @throws ResourceNotFoundException if the table does not exist
     */
    public TableDesc getTableDesc(String schemaName, String tableName) 
            throws ResourceNotFoundException {
        // create the table description
        TableDesc ingestTable;
        try {
            ingestTable = createTableDesc(schemaName, tableName);
        } catch (SQLException e) {
            throw new IllegalArgumentException(String.format("error getting database metadata for %s because: %s",
                    tableName, e.getMessage()));
        }

        // make caller the table owner
        Subject caller = AuthenticationUtil.getCurrentSubject();
        TapPermissions tapPermissions = new TapPermissions();
        ingestTable.tapPermissions = tapPermissions;
        
        return ingestTable;
    }

    private TableDesc createTableDesc(String schemaName, String tableName)
            throws SQLException, ResourceNotFoundException {
        // get the table metadata
        String s = getUnqualifiedTableNameFromTable(tableName);
        final String internalTableName = databaseDataType.toInternalDatabaseObjectName(s);
        final String internalSchemaName = databaseDataType.toInternalDatabaseObjectName(schemaName);
        log.debug(String.format("creating TableDesc for %s %s aka %s", internalSchemaName, internalTableName, tableName));
        Connection conn = dataSource.getConnection();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet rs = databaseMetaData.getTables(null, internalSchemaName, internalTableName, null);
            if (rs != null && !rs.next()) {
                log.debug("table does not exist: " + tableName);
                throw new ResourceNotFoundException("database table not found: " + tableName);
            }

            log.debug(String.format("querying DatabaseMetadata for schema=%s table=%s", internalSchemaName, internalTableName));
            //TODO too pg specific? table names are stored lower case in the system tables queried for the metadata
            ResultSet indexInfo = databaseMetaData.getIndexInfo(null, internalSchemaName, internalTableName, false, false);
            List<String> indexedColumns = new ArrayList<String>();
            try {
                while (indexInfo.next()) {
                    String indexedColumn = indexInfo.getString("COLUMN_NAME");
                    indexedColumns.add(indexedColumn);
                    log.debug("indexed column: " + indexedColumn);
                }
            } finally {
                indexInfo.close();
            }

            // build TableDesc
            TableDesc tableDesc = new TableDesc(schemaName, tableName); // as specified by caller
            tableDesc.tableType = TableDesc.TableType.TABLE;
            log.debug(String.format("creating TableDesc %s %s aka %s", internalSchemaName, internalTableName, tableName));
            //TODO too pg specific? table names are stored lower case in the system tables queried for the metadata
            ResultSet columnInfo = databaseMetaData.getColumns(null, internalSchemaName, internalTableName, null);
            try {
                while (columnInfo.next()) {
                    String columnName = columnInfo.getString("COLUMN_NAME");
                    String columnType = columnInfo.getString("TYPE_NAME");
                    TapDataType tapDataType = databaseDataType.toTapDataType(columnType, null);
                    if (TapDataType.CHAR.getDatatype().equals(tapDataType.getDatatype()) && tapDataType.xtype == null) {
                        Integer colSize = columnInfo.getInt("COLUMN_SIZE"); // int
                        if (colSize == 1) {
                            colSize = null; // length 1 means scalar in TAP
                        }
                        tapDataType = databaseDataType.toTapDataType(columnType, colSize);
                    }
                    log.debug(String.format("creating ColumnDesc %s %s %s", tableName, columnName, tapDataType));
                    ColumnDesc columnDesc = new ColumnDesc(tableName, columnName, tapDataType);
                    columnDesc.indexed = indexedColumns.contains(columnName);
                    tableDesc.getColumnDescs().add(columnDesc);
                }
            } finally {
                columnInfo.close();
            }
            return tableDesc;
        } finally {
            conn.close();
        }
    }

    String getUnqualifiedTableNameFromTable(String tableName) {
        String[] st = tableName.split("[.]");
        if (st.length == 2) {
            return st[1];
        }
        throw new IllegalArgumentException("invalid table name: " + tableName + " (expected: <schema>.<table>)");
    }

}
