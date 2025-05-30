/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.schema.TapSchemaLoader;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Implementation of the JobRunner interface from the UWS framework. This is the
 * main class that implements TAP semantics; it is usable to execute queries with 
 * both the async and sync configurations.
 *
 * <p>This class dynamically loads and uses implementation classes using the 
 * the PluginFactory class configured via the PluginFactory.properties file. This 
 * allows one to control the behavior of several key components.
 *
 * <p>This class uses JDNI to find java.sql.DataSource instances for
 * executing database statements. By default, a datasource named <code>jdbc/tapuser</code> is 
 * found via JNDI and used for queries, reading tap_schema tables, and creating
 * temporary tables in the tap_upload schema. 
 *
 * @author pdowler
 */
public class QueryRunner implements JobRunner {

    private static final Logger log = Logger.getLogger(QueryRunner.class);

    private static final String TAPUSER_DATASOURCE_NAME = "jdbc/tapuser";

    protected Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private WebServiceLogInfo logInfo;
    
    // intermediate state for the UWS JobExecutor to access when job is HELD
    public final transient Map<String,TableDesc.TableLocationInfo> uploadTableLocations = new TreeMap<>();
    public transient List<TapSelectItem> selectList;
    public transient VOTableDocument resultTemplate;
    public transient String internalSQL;
    public transient Integer maxRows;
    protected final boolean returnHELD;
    
    private final int responseCodeOnUserFail = 400;
    private final int responseCodeOnPermissionDenied = 403;
    private final int responseCodeOnSystemFail = 500;

    public QueryRunner() {
        this.returnHELD = false;
    }
    
    protected QueryRunner(boolean returnHELD) {
        this.returnHELD = returnHELD;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
    }

    @Override
    public void setJobUpdater(JobUpdater ju) {
        this.jobUpdater = ju;
    }

    @Override
    public void setSyncOutput(SyncOutput so) {
        this.syncOutput = so;
    }

    @Override
    public void run() {
        logInfo = new JobLogInfo(job);
        log.info(logInfo.start());
        long start = System.currentTimeMillis();

        try {
            runImpl();
        } catch (Throwable ex) {
            log.error("BUG: unexpected exception", ex);
        }

        logInfo.setElapsedTime(System.currentTimeMillis() - start);
        log.info(logInfo.end());
    }

    protected PluginFactoryImpl getPluginFactory() {
        return new PluginFactoryImpl(job);
    }

    /**
     * Get the DataSource to be used to execute the query. By default, this uses JNDI to
     * find an app-server supplied DataSource named <code>jdbc/tapuser</code>.
     *
     * @return DataSource for executing the user query
     * @throws Exception
     */
    protected DataSource getQueryDataSource()
            throws Exception {
        log.debug("find DataSource via JNDI lookup...");
        Context initContext = new InitialContext();
        Context envContext = (Context) initContext.lookup("java:comp/env");
        return (DataSource) envContext.lookup(TAPUSER_DATASOURCE_NAME);
    }

    /**
     * Get the DataSource to be used to query the <code>tap_schema</code>.
     * By default, this calls getQueryDataSource().
     *
     * @return DataSource for reading the tap_schema content
     * @throws Exception
     */
    protected DataSource getTapSchemaDataSource() throws Exception {
        return getQueryDataSource();
    }

    /**
     * Get the DataSource to be used to insert uploaded tables into the database.
     * By default, this calls getQueryDataSource().
     *
     * @return DataSource for creating tables in the <code>tap_upload</code> schema
     * @throws Exception
     */
    protected DataSource getUploadDataSource() throws Exception {
        return getQueryDataSource();
    }

    private void runImpl() {
        List<Result> diagnostics = new ArrayList<>();

        long t1 = System.currentTimeMillis();
        long t2;
        long dt;

        log.debug("run: " + job.getID());
        List<Parameter> paramList = job.getParameterList();
        log.debug("job " + job.getID() + ": " + paramList.size() + " parameters");
        PluginFactoryImpl pfac = getPluginFactory();
        log.debug("loaded: " + pfac);

        ResultStore rs = null;
        if (syncOutput == null) {
            rs = pfac.getResultStore();
            log.debug("loaded: " + rs.getClass().getName());
        }
        
        try {
            log.debug("try: QUEUED -> EXECUTING...");
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if (!ExecutionPhase.EXECUTING.equals(ep)) {
                ep = jobUpdater.getPhase(job.getID());
                log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- phase is " + ep);
                logInfo.setSuccess(false);
                logInfo.setMessage("Could not set job phase to EXECUTING.");
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");

            t2 = System.currentTimeMillis();
            dt = t2 - t1;
            t1 = t2;
            diagnostics.add(new Result("diag", URI.create("uws:executing:" + dt)));

            // start processing the job
            
            final DataSource tapSchemaDataSource = getTapSchemaDataSource();
            if (tapSchemaDataSource == null) {
                throw new RuntimeException("failed to find the tap_schema DataSource");
            }
            // this one is optional, so take care
            DataSource uploadDataSource = null;
            try {
                uploadDataSource = getUploadDataSource();
            } catch (NameNotFoundException nex) {
                log.debug(nex.toString());
            }

            t2 = System.currentTimeMillis();
            dt = t2 - t1;
            t1 = t2;
            diagnostics.add(new Result("diag", URI.create("jndi:lookup:" + dt)));

            log.debug("reading TapSchema...");
            TapSchemaDAO dao = pfac.getTapSchemaDAO();
            dao.setDataSource(tapSchemaDataSource);
            dao.setOrdered(true); // make AllColumnConverter respect column_index
            TapSchemaLoader loader = new TapSchemaLoader(dao);
            TapSchema tapSchema = loader.load();

            t2 = System.currentTimeMillis();
            dt = t2 - t1;
            t1 = t2;
            diagnostics.add(new Result("diag", URI.create("read:tap_schema:" + dt)));

            log.debug("checking uploaded tables...");
            UploadManager uploadManager = pfac.getUploadManager();
            uploadManager.setDataSource(uploadDataSource);
            uploadManager.setDatabaseDataType(pfac.getDatabaseDataType());
            Map<String, TableDesc> tableDescs = uploadManager.upload(paramList, job.getID());
            if (tableDescs != null && !tableDescs.isEmpty()) {
                log.debug("adding TAP_UPLOAD SchemaDesc to TapSchema...");
                SchemaDesc tapUploadSchema = new SchemaDesc(uploadManager.getUploadSchema());
                tapUploadSchema.getTableDescs().addAll(tableDescs.values());
                tapSchema.getSchemaDescs().add(tapUploadSchema);

                for (Map.Entry<String, TableDesc> e : tableDescs.entrySet()) {
                    if (e.getValue().dataLocation != null) {
                        uploadTableLocations.put(e.getKey(), e.getValue().dataLocation);
                    }
                }
            }

            log.debug("invoking MaxRecValidator...");
            MaxRecValidator maxRecValidator = pfac.getMaxRecValidator();
            maxRecValidator.setTapSchema(tapSchema);
            maxRecValidator.setJob(job);
            maxRecValidator.setSynchronousMode(syncOutput != null);
            this.maxRows = maxRecValidator.validate();

            log.debug("creating TapQuery implementation...");
            TapQuery query = pfac.getTapQuery();
            query.setTapSchema(tapSchema);
            query.setExtraTables(tableDescs);
            if (maxRows != null) {
                if (maxRows == Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Invalid MAXREC: " + maxRows + " (max " + (Integer.MAX_VALUE - 1) + ")");
                }
                query.setMaxRowCount(maxRows + 1); // +1 so the TableWriter can detect overflow
            }

            log.debug("invoking TapQuery implementation: " + query.getClass().getCanonicalName());
            this.selectList = query.getSelectList();
            final String queryInfo = query.getInfo();
            this.internalSQL = query.getSQL();

            log.debug("creating TapTableWriter...");
            TableWriter tableWriter = pfac.getTableWriter();
            tableWriter.setSelectList(selectList);
            tableWriter.setQueryInfo(queryInfo);
            this.resultTemplate = tableWriter.generateOutputTable();
            t2 = System.currentTimeMillis();
            dt = t2 - t1;
            t1 = t2;
            diagnostics.add(new Result("diag", URI.create("query:parse:" + dt)));

            // returnHELD only applies to content queries
            if (!query.isTapSchemaQuery() && returnHELD) {
                ExecutionPhase held = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.HELD, new Date());
                log.debug(job.getID() + ": EXECUTING -> HELD: " + held);
                if (!ExecutionPhase.HELD.equals(held)) {
                    logInfo.setSuccess(false);
                    logInfo.setMessage("Could not set job phase to HELD.");
                }
                return;
            }
            
            // prepare to execute the querry
            DataSource queryDataSource = getQueryDataSource();
            if (queryDataSource == null) {
                throw new RuntimeException("failed to find the query DataSource");
            }
            
            Connection connection = null;
            PreparedStatement pstmt = null;
            ResultSet resultSet = null;
            URL url = null;
            try {
                if (maxRows == null || maxRows > 0) {
                    log.debug("getting database connection...");
                    if (query.isTapSchemaQuery()) {
                        log.debug("tap_schema query");
                        connection = tapSchemaDataSource.getConnection();
                    } else {
                        log.debug("regular query");
                        connection = queryDataSource.getConnection();
                    }

                    t2 = System.currentTimeMillis();
                    dt = t2 - t1;
                    t1 = t2;
                    diagnostics.add(new Result("diag", URI.create("jndi:connect:" + dt)));

                    // make fetch size (client batch size) small,
                    // and restrict to forward only so that client memory usage is minimal since
                    // we are only interested in reading the ResultSet once
                    log.debug("setAutoCommit: " + pfac.getAutoCommit());
                    connection.setAutoCommit(pfac.getAutoCommit());

                    log.debug("executing query: " + internalSQL);
                    pstmt = connection.prepareStatement(internalSQL);
                    pstmt.setFetchDirection(ResultSet.FETCH_FORWARD);
                    if (maxRows == null || maxRows > 1000) {
                        log.debug("maxRows = " + maxRows + ": setting fetchSize = 1000");
                        pstmt.setFetchSize(1000);
                    } else {
                        log.debug("maxRows = " + maxRows + ": not setting fetchSize");
                    }
                    resultSet = pstmt.executeQuery();
                    log.debug("result set: " + resultSet.getClass().getName());
                }

                t2 = System.currentTimeMillis();
                dt = t2 - t1;
                t1 = t2;
                diagnostics.add(new Result("diag", URI.create("query:execute:" + dt)));

                String filename = "result_" + job.getID() + "." + tableWriter.getExtension();
                String contentType = tableWriter.getContentType();

                if (syncOutput != null) {

                    log.debug("streaming output: " + contentType);
                    syncOutput.setHeader("Content-Type", contentType);
                    String disp = "inline; filename=\"" + filename + "\"";
                    syncOutput.setHeader("Content-Disposition", disp);
                    if (maxRows == null) {
                        tableWriter.write(resultSet, syncOutput.getOutputStream());
                    } else {
                        tableWriter.write(resultSet, syncOutput.getOutputStream(), maxRows.longValue());
                    }

                    t2 = System.currentTimeMillis();
                    dt = t2 - t1;
                    t1 = t2;
                    diagnostics.add(new Result("diag", URI.create("query:stream:" + dt)));
                } else if (rs != null) {
                    ep = jobUpdater.getPhase(job.getID());
                    if (ExecutionPhase.ABORTED.equals(ep)) {
                        log.debug(job.getID() + ": found phase = ABORTED before writing results - DONE");
                        return;
                    }

                    log.debug("result filename: " + filename);
                    rs.setJob(job);
                    rs.setFilename(filename);
                    rs.setContentType(contentType);
                    url = rs.put(resultSet, tableWriter, maxRows);

                    t2 = System.currentTimeMillis();
                    dt = t2 - t1;
                    t1 = t2;
                    diagnostics.add(new Result("diag", URI.create("query:store:" + dt)));
                } else {
                    throw new RuntimeException("BUG: both syncOutput and ResultStore are null");
                }

                log.debug("executing query... " + tableWriter.getRowCount() + " rows [OK]");
                // note: final chosen here because we could in theory write intermediate rowcounts or state
                // as suggested by Dave Morris 
                diagnostics.add(new Result("rowcount", URI.create("final:" + tableWriter.getRowCount())));
            } catch (SQLException ex) {
                log.error("SQL Execution error.", ex);
                throw ex;
            } finally {
                if (connection != null) {
                    try {
                        if (!pfac.getAutoCommit()) {
                            connection.setAutoCommit(true);
                        }
                    } catch (Throwable ignore) {
                        log.debug("failed to set connection auto commit = true", ignore);
                    }
                    try {
                        if (resultSet != null) {
                            resultSet.close();
                        }
                    } catch (Throwable ignore) {
                        log.debug("failed to close resultset", ignore);
                    }
                    try {
                        if (pstmt != null) {
                            pstmt.close();
                        }
                    } catch (Throwable ignore) {
                        log.debug("failed to close prepared statement", ignore);
                    }
                    try {
                        connection.close();
                    } catch (Throwable ignore) {
                        log.debug("failed to close connection", ignore);
                    }
                }
            }

            if (syncOutput != null) {
                log.debug("[sync] setting ExecutionPhase = " + ExecutionPhase.COMPLETED + " diag: " + diagnostics.size());
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, diagnostics, new Date());
            } else {
                try {
                    Result res = new Result("result", new URI(url.toExternalForm()));
                    diagnostics.add(res);
                    log.debug("[async] setting ExecutionPhase = " + ExecutionPhase.COMPLETED + " result+diag: " + diagnostics.size());
                    jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, diagnostics, new Date());
                } catch (URISyntaxException e) {
                    log.error("BUG: URL is not a URI: " + url.toExternalForm(), e);
                    throw e;
                }
            }
        } catch (Throwable t) {
            logInfo.setMessage(t.getMessage());
            int errorCode;
            if (t instanceof IllegalArgumentException || t instanceof UnsupportedOperationException) {
                logInfo.setSuccess(true);
                errorCode = responseCodeOnUserFail;
            } else if (t instanceof AccessControlException) {
                logInfo.setSuccess(true);
                errorCode = responseCodeOnPermissionDenied;
            } else {
                logInfo.setSuccess(false);
                errorCode = responseCodeOnSystemFail;
            }
            String errorMessage = null;
            URL errorURL = null;
            try {
                t2 = System.currentTimeMillis();
                dt = t2 - t1;
                t1 = t2;
                diagnostics.add(new Result("diag", URI.create("fail:" + dt)));

                errorMessage = t.getClass().getSimpleName() + ":" + t.getMessage();
                log.debug("BADNESS", t);
                log.debug("Error message: " + errorMessage);

                log.debug("creating TableWriter for error...");
                TableWriter ewriter = pfac.getErrorWriter();

                // write to buffer so we can determine content-length
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ewriter.write(t, bos);
                String emsg = bos.toString();
                String filename = "error_" + job.getID() + "." + ewriter.getExtension();
                if (syncOutput != null) {
                    syncOutput.setCode(errorCode);
                    syncOutput.setHeader("Content-Type", ewriter.getErrorContentType());
                    syncOutput.setHeader("Content-Length", Integer.toString(emsg.length()));
                    String disp = "inline; filename=\"" + filename + "\"";
                    syncOutput.setHeader("Content-Disposition", disp);
                    Writer w = new OutputStreamWriter(syncOutput.getOutputStream());
                    w.write(emsg);
                    w.flush();

                    t2 = System.currentTimeMillis();
                    dt = t2 - t1;
                    t1 = t2;
                    diagnostics.add(new Result("diag", URI.create("fail:stream:" + dt)));
                } else if (rs != null) {
                    rs.setJob(job);
                    rs.setFilename(filename);
                    rs.setContentType(ewriter.getContentType());
                    errorURL = rs.put(t, ewriter);

                    t2 = System.currentTimeMillis();
                    dt = t2 - t1;
                    t1 = t2;
                    diagnostics.add(new Result("diag", URI.create("fail:store:" + dt)));
                } else {
                    throw new RuntimeException("BUG: both syncOutput and ResultStore are null");
                }

                log.debug("Error URL: " + errorURL);
                ErrorSummary es = new ErrorSummary(errorMessage, ErrorType.FATAL, errorURL);
                log.debug("setting ExecutionPhase = " + ExecutionPhase.ERROR);
                // TODO: add diagnostics to final job state when failing; requires cadc-uws-server support
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            } catch (Throwable th2) {
                log.error("failed to persist error", th2);
                t2 = System.currentTimeMillis();
                dt = t2 - t1;
                t1 = t2;
                diagnostics.add(new Result("diag", URI.create("fail:fail:store:" + dt)));
                // this is really bad: try without the document
                log.debug("setting ExecutionPhase = " + ExecutionPhase.ERROR);
                ErrorSummary es = new ErrorSummary(errorMessage, ErrorType.FATAL);
                try {
                    // TODO: add diagnostics to final job state when failing; requires cadc-uws-server support
                    jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
                } catch (Throwable t3) {
                    log.debug("failed to update job from executing to error", t3);
                }
            }
        } finally {
            // temporary visibility into this intermediate state
            try {
                log.debug("job " + job.getID() + " DONE");
                if (uploadTableLocations != null) {
                    log.debug("stored upload tables: " + uploadTableLocations.size());
                    for (Map.Entry<String,TableDesc.TableLocationInfo> e : uploadTableLocations.entrySet()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("\ttable: ").append(e.getKey()).append(" -> ");
                        for (Map.Entry<String,URI> me : e.getValue().map.entrySet()) {
                            sb.append("\n\t\t").append(me.getKey()).append(": ").append(me.getValue());
                        }
                        log.debug(sb.toString());
                    }
                }
                if (selectList != null) {
                    log.debug("select list: " + selectList.size() + " columns");
                }
                if (resultTemplate != null) {
                    log.debug("result template:\n");
                    VOTableWriter w = new VOTableWriter();
                    StringWriter sw = new StringWriter();
                    w.write(resultTemplate, sw);
                    log.debug(sw.toString());
                }
                log.debug("effective maxRows: " + maxRows);
                log.debug("internal SQL:\n" + internalSQL);
            } catch (Exception oops) {
                log.error("BUG: ", oops);
            }
        }
    }

}
