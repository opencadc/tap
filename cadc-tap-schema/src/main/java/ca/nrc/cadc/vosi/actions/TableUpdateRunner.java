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

package ca.nrc.cadc.vosi.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.ParamExtractor;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableIngester;
import ca.nrc.cadc.tap.schema.ADQLIdentifierException;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * TableUpdateRunner can be used for UWS async jobs that modify a table.
 * Supported table modifications: 
 * <ul>
 * <li> create (unique) index on  a single column </li>
 * <li> ingest and existing database table into the tap_schema</li>
 * </ul>
 * @author pdowler
 */
public class TableUpdateRunner implements JobRunner {

    private static final Logger log = Logger.getLogger(TableUpdateRunner.class);

    /**
     * The default JNDI DataSource name is jdbc/tapadm.
     */
    public static final String DEFAULT_DATASOURCE = "jdbc/tapadm";
    
    private static final List<String> PARAM_NAMES = new ArrayList<String>();

    static {
        PARAM_NAMES.add("index");
        PARAM_NAMES.add("ingest");
        PARAM_NAMES.add("table");
        PARAM_NAMES.add("unique");
    }

    private JobUpdater jobUpdater;
    private WebServiceLogInfo logInfo;
    private boolean readable = true;
    private boolean writable = true;
    protected Job job;
    
    public TableUpdateRunner() {
    }

    @Override
    public void setAppName(String appName) {
        String key = appName + RestAction.STATE_MODE_KEY;
        String val = System.getProperty(key);
        log.debug("initState: " + key + "=" + val);
        if (RestAction.STATE_OFFLINE.equals(val)) {
            this.readable = false;
            this.writable = false;
        } else if (RestAction.STATE_READ_ONLY.equals(val)) {
            this.writable = false;
        }
        log.debug("setAppName: " + appName + " " + key + "=" + val + " -> " + readable + "," + writable);
    }

    @Override
    public void setJobUpdater(JobUpdater ju) {
        this.jobUpdater = ju;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
    }

    @Override
    public void setSyncOutput(SyncOutput so) {
        //no-op: not intended for sync use
    }

    @Override
    public void run() {
        logInfo = new JobLogInfo(job);
        
        log.info(logInfo.start());
        long start = System.currentTimeMillis();

        doit();

        logInfo.setElapsedTime(System.currentTimeMillis() - start);
        log.info(logInfo.end());
    }

    private void doit() {
        try {
            try {
                ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
                if (!ExecutionPhase.EXECUTING.equals(ep)) {
                    ep = jobUpdater.getPhase(job.getID());
                    log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- phase is " + ep);
                    logInfo.setSuccess(false);
                    logInfo.setMessage("Could not set job phase to EXECUTING.");
                    return;
                }
                log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");
                
                // check service state
                if (!writable) {
                    if (readable) {
                        throw new TransientException(RestAction.STATE_READ_ONLY_MSG, 180);
                    }
                    throw new TransientException(RestAction.STATE_OFFLINE_MSG, 180);
                }
                
                // check for the requested operation
                ParamExtractor pe = new ParamExtractor(PARAM_NAMES);
                Map<String, List<String>> params = pe.getParameters(job.getParameterList());
                String index = getSingleValue("index", params);
                String ingest = getSingleValue("ingest", params);
                if (index == null && ingest == null) {
                    throw new IllegalArgumentException("one of 'index' or 'ingest' parameter must be specified");
                } else if (index != null && ingest != null) {
                    throw new IllegalArgumentException("'index' and 'ingest' parameters cannot be specified at the same time");
                }
                if (index != null) {
                    indexTable(params);
                } else {
                    ingestTable(params);
                }

                ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, new Date());
                logInfo.setSuccess(true);
            } catch (AccessControlException | IllegalArgumentException | ResourceNotFoundException ex) {
                logInfo.setMessage(ex.getMessage());
                logInfo.setSuccess(true);
                ErrorSummary es = new ErrorSummary(ex.getMessage(), ErrorType.FATAL);
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            } catch (TransientException ex) {
                logInfo.setMessage(ex.getMessage());
                logInfo.setSuccess(true);
                ErrorSummary es = new ErrorSummary(ex.getMessage(), ErrorType.TRANSIENT);
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            } catch (RuntimeException ex) {
                logInfo.setMessage(ex.getMessage());
                logInfo.setSuccess(false);
                ErrorSummary es = new ErrorSummary("unexpected failure: " + ex.getMessage(), ErrorType.FATAL);
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            }
        } catch (Throwable unexpected) {
            logInfo.setMessage(unexpected.getMessage());
            logInfo.setSuccess(false);
            try {
                ErrorSummary es = new ErrorSummary("unexpected failure: " + unexpected, ErrorType.FATAL);
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            } catch (Exception ex) {
                log.error("failed to set job to error state", ex);
            }
        }
    }

    /**
     * Add a index to a column in the tap_schema and create the index in the database.
     * @param params list of request query parameters.
     */
    protected void indexTable(Map<String, List<String>> params) {
        String tableName = getSingleValue("table", params);
        String columnName = getSingleValue("index", params);
        boolean unique = "true".equals(getSingleValue("unique", params));
        log.debug(String.format("indexing table=%s column=%s unique=%s", tableName, columnName, unique));

        if (tableName == null) {
            throw new IllegalArgumentException("missing parameter 'table'");
        }

        PluginFactory pf = new PluginFactory();
        TapSchemaDAO ts = pf.getTapSchemaDAO();
        DataSource ds = getDataSource();
        ts.setDataSource(ds);
        try {
            log.debug("Checking table write permission");
            TablesAction.checkTableWritePermissions(ts, tableName, logInfo);
        } catch (ResourceNotFoundException | IOException ex) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }

        TableDesc td = ts.getTable(tableName);
        if (td == null) {
            // if this was not thrown in permission check above then we have an inconsistency between
            // the tap_schema content and the table ownership
            log.error("INCONSISTENT STATE: permission check says table " + tableName + "exists but it is not in tap_schema");
            throw new IllegalArgumentException("table not found: " + tableName);
        }

        ColumnDesc cd = td.getColumn(columnName);
        if (cd == null) {
            throw new IllegalArgumentException("column not found: " + columnName + " in table " + tableName);
        }
        if (cd.indexed) {
            throw new IllegalArgumentException("column is already indexed: " + columnName + " in table " + tableName);
        }

        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            tm.startTransaction();

            // create index
            TableCreator tc = new TableCreator(ds);
            tc.createIndex(cd, unique);

            // createIndex can take considerable time so our view of the column metadata could be out of date

            // write lock row in tap_schema.columns
            ts.put(cd);

            // get current values in case another thread has updated it
            cd = ts.getColumn(tableName, cd.getColumnName());

            // update tap_schema
            cd.indexed = true;
            ts.put(cd);

            tm.commitTransaction();
        } catch (Exception ex) {
            boolean dbg = false;
            if (ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException) {
                dbg = true;
            }
            try {
                if (dbg) {
                    log.debug("create index and update tap_schema failed - rollback", ex);
                } else {
                    log.error("create index and update tap_schema failed - rollback", ex);
                }
                tm.rollbackTransaction();
                if (dbg) {
                    log.debug("create index and update tap_schema failed - rollback: OK");
                } else {
                    log.error("create index and update tap_schema failed - rollback: OK");
                }
            } catch (Exception oops) {
                log.error("create index and update tap_schema - rollback : FAIL", oops);
            }
            if (ex instanceof IllegalArgumentException) {
                throw ex;
            }
            throw new RuntimeException("failed to update table " + tableName + " reason: " + ex.getMessage(), ex);
        } finally {
            if (tm.isOpen()) {
                log.error("BUG: open transaction in finally - trying to rollback");
                try {
                    tm.rollbackTransaction();
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    /**
     * Add the metadata for an existing table to the tap_schema.
     *
     * @param params list of request query parameters.
     * @throws IllegalArgumentException if the ingested table is invalid
     * @throws ResourceNotFoundException if the target table is not found
     */
    protected void ingestTable(Map<String, List<String>> params) 
            throws IllegalArgumentException, ResourceNotFoundException {
        boolean ingest = "true".equals(getSingleValue("ingest", params));
        if (!ingest) {
            throw new IllegalStateException("'ingest' parameter specified but value is 'false', ingest cancelled");
        }

        String tableName = getSingleValue("table", params);
        if (tableName == null) {
            throw new IllegalArgumentException("missing parameter 'table'");
        }
        log.debug("ingesting table " + tableName);

        PluginFactory pf = new PluginFactory();
        TapSchemaDAO tapSchemaDAO = pf.getTapSchemaDAO();
        DataSource ds = getDataSource();
        tapSchemaDAO.setDataSource(ds);

        // check write permissions to the tap_schema
        String schemaName = Util.getSchemaFromTable(tableName);
        try {
            TablesAction.checkSchemaWritePermissions(tapSchemaDAO, schemaName, logInfo);
        }  catch (ResourceNotFoundException | IOException ex) {
            throw new IllegalArgumentException("ingest schema not found in tap_schema: " + schemaName);
        }

        log.debug("check if table already exists in tap_schema");
        TableDesc tableDesc = tapSchemaDAO.getTable(tableName);
        if (tableDesc != null) {
            throw new IllegalArgumentException("ingest table already exists in tap_schema: " + tableName);
        }

        // note: this is outside the transaction because it uses low-level db to get
        // database metadata
        TableIngester tableIngester = new TableIngester(ds);
        log.debug("read table from database");
        TableDesc ingestable = tableIngester.getTableDesc(schemaName, tableName);
        // check the table is valid ADQL name
        try {
            TapSchemaUtil.checkValidTableName(ingestable.getTableName());
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid table name: " + ingestable.getTableName(), ex);
        }
        try {
            for (ColumnDesc cd : ingestable.getColumnDescs()) {
                TapSchemaUtil.checkValidIdentifier(cd.getColumnName());
            }
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
            
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            log.debug("start transaction");
            tm.startTransaction();
            
            // assign owner
            ingestable.tapPermissions.owner = AuthenticationUtil.getCurrentSubject();
            ingestable.apiCreated = false; // pre-existing table
            
            log.debug("put table to tap_schema");
            tapSchemaDAO.put(ingestable);
            log.debug(String.format("added table '%s' to tap_schema", tableName));

            log.debug("commit transaction");
            tm.commitTransaction();
        } catch (Exception ex) {
            
            try {
                log.error("ingest table and update tap_schema failed - rollback", ex);
                tm.rollbackTransaction();
                log.error("ingest table and update tap_schema failed - rollback: OK");
            } catch (Exception oops) {
                log.error("ingest table and update tap_schema - rollback : FAIL", oops);
            }
            throw new RuntimeException("failed to ingest table " + tableName + " reason: " + ex.getMessage(), ex);
        } finally {
            if (tm.isOpen()) {
                log.error("BUG: open transaction in finally - trying to rollback");
                try {
                    tm.rollbackTransaction();
                    log.error("BUG: rollback in finally: OK");
                } catch (Exception oops) {
                    log.error("BUG: rollback in finally: FAIL", oops);
                }
                throw new RuntimeException("BUG: open transaction in finally");
            }
        }
    }

    private String getSingleValue(String pname, Map<String, List<String>> params) {
        List<String> vals = params.get(pname);
        if (vals == null || vals.isEmpty()) {
            return null;
        }
        if (vals.size() > 1) {
            throw new IllegalArgumentException("invalid input: found " + vals.size() + " values for " + pname + " -- expected 1");
        }
        return vals.get(0);
    }

    /**
     * By default this method uses JNDI to find the default data source.
     * @return data source to use for table modifications
     */
    protected DataSource getDataSource() {
        try {
            return DBUtil.findJNDIDataSource(DEFAULT_DATASOURCE);
        } catch (NamingException ex) {
            throw new RuntimeException("CONFIG: failed to find datasource " + DEFAULT_DATASOURCE, ex);
        }
    }
}
