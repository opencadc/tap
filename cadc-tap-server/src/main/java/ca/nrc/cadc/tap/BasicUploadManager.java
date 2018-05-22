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

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.stc.Position;
import ca.nrc.cadc.stc.Region;
import ca.nrc.cadc.stc.StcsParsingException;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.JDOMVOTableParser;
import ca.nrc.cadc.tap.upload.UploadParameters;
import ca.nrc.cadc.tap.upload.UploadTable;
import ca.nrc.cadc.tap.upload.VOTableParser;
import ca.nrc.cadc.tap.upload.VOTableParserException;
import ca.nrc.cadc.tap.upload.datatype.DatabaseDataType;
import ca.nrc.cadc.tap.upload.datatype.TapConstants;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

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
    protected int maxUploadRows;

    protected Job job;
    
    /**
     * Default constructor.
     */
    private BasicUploadManager() { }
    
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

        // Statements
        Statement stmt = null;
        PreparedStatement ps = null;
        Connection con = null;
        boolean txn = false;
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

            // acquire connection
            con = dataSource.getConnection();

            con.setAutoCommit(false);
            txn = true;

            // Process each table.
            for (UploadTable uploadTable : uploadParameters.uploadTables)
            {
                cur = uploadTable;
                
                // XML parser
                // TODO: make configurable.
                log.debug(uploadTable);
                
                VOTableParser parser = getVOTableParser(uploadTable);

                // Get the Table description.
                TableDesc tableDesc = parser.getTableDesc();
                sanitizeTable(tableDesc);

                // Fully qualified name of the table in the database.
                String databaseTableName = getDatabaseTableName(uploadTable);

                metadata.put(databaseTableName, tableDesc);
                log.debug("upload table: " + databaseTableName + " aka " + tableDesc);
                
                // Build the SQL to create the table.
                String tableSQL = getCreateTableSQL(tableDesc, databaseTableName, databaseDataType);
                log.debug("Create table SQL: " + tableSQL);

                // Create the table.
                stmt = con.createStatement();
                stmt.executeUpdate(tableSQL);
                
                // Grant select access for others to query.
                String grantSQL = getGrantSelectTableSQL(databaseTableName);
                if (grantSQL != null && !grantSQL.isEmpty())
                {
                    log.debug("Grant select SQL: " + grantSQL);
                    stmt.executeUpdate(grantSQL);
                }
                
                // commit the create and grant
                con.commit();

                // Get a PreparedStatement that populates the table.
                String insertSQL = getInsertTableSQL(tableDesc, databaseTableName); 
                ps = con.prepareStatement(insertSQL);
                log.debug("Insert table SQL: " + insertSQL);

                // Populate the table from the VOTable tabledata rows.
                int numRows = 0;
                Iterator<List<Object>> it = parser.iterator();
                while (it.hasNext())
                {
                    // Get the data for the next row.
                    List<Object> row = it.next();

                    // Update the PreparedStatement with the row data.
                    updatePreparedStatement(ps, databaseDataType, tableDesc.getColumnDescs(), row);

                    // Execute the update.
                    ps.executeUpdate();
                    
                    // commit every NUM_ROWS_PER_COMMIT rows
                    if (numRows != 0 && (numRows % NUM_ROWS_PER_COMMIT) == 0)
                    {
                        log.debug(NUM_ROWS_PER_COMMIT + " rows committed");
                        con.commit();
                    }
                    
                    // Check if we've reached exceeded the max number of rows.
                    numRows++;
                    if (numRows == maxUploadRows)
                        throw new UnsupportedOperationException("Exceded maximum number of allowed rows: " + maxUploadRows);
                }
                
                // Commit remaining rows.
                con.commit();
                
                log.debug(numRows + " rows inserted into " + databaseTableName);
            }
            txn = false;
        }
        catch(StcsParsingException ex)
        {
            throw new RuntimeException("failed to parse table " + cur.tableName + " from " + cur.uri, ex);
        }
        catch(VOTableParserException ex)
        {
            throw new RuntimeException("failed to parse table " + cur.tableName + " from " + cur.uri, ex);
        }
        catch(IOException ex)
        {
            throw new RuntimeException("failed to read table " + cur.tableName + " from " + cur.uri, ex);
        }
        catch (SQLException e)
        {
            throw new RuntimeException("failed to create and load table in DB", e);
        }
        finally
        {
            try
            {
                if (con != null)
                    con.rollback();
            }
            catch (SQLException ignore) { }
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException ignore) { }
            }
            if (ps != null)
            {
                try
                {
                    ps.close();
                }
                catch (SQLException ignore) { }
            }
            if (con != null)
            {
                try
                {
                    con.close();
                }
                catch (SQLException ignore) { }
            }
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
    
    /**
     * Create the SQL required to create a table described by the TableDesc.
     *
     * @param tableDesc describes the table.
     * @param databaseTableName fully qualified table name.
     * @param databaseDataType map of SQL types to database specific data types.
     * @return SQL to create the table.
     * @throws SQLException
     */
    protected String getCreateTableSQL(TableDesc tableDesc, String databaseTableName, DatabaseDataType databaseDataType)
        throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("create table ");
        sb.append(databaseTableName);
        sb.append(" ( ");
        for (int i = 0; i < tableDesc.getColumnDescs().size(); i++)
        {
            ColumnDesc columnDesc = tableDesc.getColumnDescs().get(i);
            sb.append(columnDesc.getColumnName());
            sb.append(" ");
            sb.append(databaseDataType.getDataType(columnDesc));
            sb.append(" null ");
            if (i + 1 < tableDesc.getColumnDescs().size())
                sb.append(", ");
        }
        sb.append(" ) ");
        return sb.toString();
    }
    
    /**
     * Create the SQL required to create a PreparedStatement
     * to insert into the table described by the TableDesc.
     * 
     * @param tableDesc describes the table.
     * @return SQL to create the table.
     */
    protected String getInsertTableSQL(TableDesc tableDesc, String databaseTableName)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        sb.append(databaseTableName);
        sb.append(" ( ");
        for (int i = 0; i < tableDesc.getColumnDescs().size(); i++)
        {
            ColumnDesc columnDesc = tableDesc.getColumnDescs().get(i);
            sb.append(columnDesc.getColumnName());
            if (i + 1 < tableDesc.getColumnDescs().size())
                sb.append(", ");
        }
        sb.append(" ) values ( ");
        for (int i = 0; i < tableDesc.getColumnDescs().size(); i++)
        {
            sb.append("?");
            if (i + 1 < tableDesc.getColumnDescs().size())
                sb.append(", ");
        }
        sb.append(" ) ");
        return sb.toString();
    }

    /**
     * Updated the PreparedStatement with the row data using the ColumnDesc to
     * determine each column data type.
     *
     * @param ps the prepared statement.
     * @param databaseDataType
     * @param columnDescs List of ColumnDesc for this table.
     * @param row Array containing the data to be inserted into the database.
     * @throws SQLException if the statement is closed or if the parameter index type doesn't match.
     */
    protected void updatePreparedStatement(PreparedStatement ps, DatabaseDataType databaseDataType,
            List<ColumnDesc> columnDescs, List<Object> row)
        throws SQLException, StcsParsingException
    {
        int i = 1;
        for (Object value : row)
        {
            ColumnDesc columnDesc = columnDescs.get(i-1);
            log.debug("update ps: " + columnDesc.getColumnName() + "[" + columnDesc.getDatatype() + "] = " + value);

            Integer sqlType = databaseDataType.getType(columnDesc);
            
            if (sqlType == null) // db-specific
            {
                Object dbv = null;
                if (value instanceof Point)
                    dbv = getPointObject((Point) value);
                else if (value instanceof Circle)
                    dbv = getCircleObject((Circle) value);
                else if (value instanceof Polygon)
                    dbv = getPolygonObject((Polygon) value);
                else if (value instanceof DoubleInterval)
                    dbv = getIntervalObject((DoubleInterval) value);
                //else if (value instanceof LongInterval)
                //    dbv = getIntervalObject((LongInterval) value);
                else if (value instanceof Position)
                    dbv = getPointObject((Position) value);
                else if (value instanceof Region)
                    dbv = getRegionObject((Region) value);
                
                ps.setObject(i, dbv); // could be null
            }
            else if (value == null) // null
                ps.setNull(i, sqlType);
            else
            {
                switch(sqlType)
                {
                    case Types.TIMESTAMP:
                        Date date = (Date) value;
                        ps.setTimestamp(i, new Timestamp(date.getTime())); // UTC
                        break;
                    default:
                        ps.setObject(i, value, sqlType);
                }
                
            }
            i++;
        }
    }

    /**
     * Convert DALI point value to an object for insert.
     *
     * @param p
     * @throws SQLException
     * @return an object suitable for use with PreparedStatement.setObject(int,Object)
     */
    protected Object getPointObject(Point p)
        throws SQLException
    {
        throw new UnsupportedOperationException("cannot convert DALI point -> internal database type");
    }
    
    /**
     * Convert DALI circle value to an object for insert.
     *
     * @param c
     * @throws SQLException
     * @return an object suitable for use with PreparedStatement.setObject(int,Object)
     */
    protected Object getCircleObject(Circle c)
        throws SQLException
    {
        throw new UnsupportedOperationException("cannot convert DALI circle -> internal database type");
    }
    
    /**
     * Convert DALI polygon value to an object for insert.
     * 
     * @param poly
     * @return an object suitable for use with PreparedStatement.setObject(int,Object)
     * @throws SQLException 
     */
    protected Object getPolygonObject(Polygon poly)
        throws SQLException
    {
        throw new UnsupportedOperationException("cannot convert DALI polygon -> internal database type");
    }
    
    /**
     * Convert DALI interval value to an object for insert.
     * 
     * @param inter
     * @return  an object suitable for use with PreparedStatement.setObject(int,Object)
     */
    protected Object getIntervalObject(DoubleInterval inter)
    {
        throw new UnsupportedOperationException("cannot convert DALI interval -> internal database type");
    }
    
    /**
     * Convert array of DALI interval values to an object for insert.
     * 
     * @param inter
     * @return  an object suitable for use with PreparedStatement.setObject(int,Object)
     */
    protected Object getIntervalArrayObject(DoubleInterval[] inter)
    {
        throw new UnsupportedOperationException("cannot convert DALI interval array -> internal database type");
    }
    
    /**
     * Convert STC-S (TAP-1.0) adql:POINT value into an object for insert.
     * 
     * @param pos
     * @return an object suitable for use with PreparedStatement.setObject(int,Object)
     * @throws SQLException 
     * 
     */
    protected Object getPointObject(Position pos)
        throws SQLException
    {
        throw new UnsupportedOperationException("cannot convert STC-S Position -> internal database type");
    }

    /**
     * Convert STC-S (TAP-1.0) adql:REGION value into an object for insert.
     *
     * @param reg
     * @throws SQLException
     * @return an object suitable for use with PreparedStatement.setObject(int,Object)
     */
    protected Object getRegionObject(Region reg)
        throws SQLException
    {
        throw new UnsupportedOperationException("cannot convert STC-S Region -> internal database type");
    }

}
