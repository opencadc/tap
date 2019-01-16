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

import ca.nrc.cadc.dali.ParamExtractor;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * TableUpdateRunner can be used for UWS async and sync jobs that modify a table.
 * Supported table modifications: 
 * <ul>
 * <li> async or sync: create (unique) index on  a single column </li>
 * </ul>
 * 
 * TODO: sync append rows from input stream, async append rows from URI
 * 
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
        PARAM_NAMES.add("table");
        PARAM_NAMES.add("index");
        PARAM_NAMES.add("unique");
    }

    private JobUpdater jobUpdater;
    private WebServiceLogInfo logInfo;

    protected Job job;
    
    public TableUpdateRunner() {
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

                ParamExtractor pe = new ParamExtractor(PARAM_NAMES);
                Map<String, List<String>> params = pe.getParameters(job.getParameterList());
                String tableName = getSingleValue("table", params);
                String columnName = getSingleValue("index", params);
                boolean unique = "true".equals(getSingleValue("unique", params));
                
                // TODO: make create index optional and check for table load from URI params
                
                if (tableName == null) {
                    throw new IllegalArgumentException("missing parameter 'table'");
                }
                if (columnName == null) {
                    throw new IllegalArgumentException("missing parameter 'index'");
                }
                
                DataSource ds = getDataSource();
                try {
                    log.debug("Checking table write permission");
                    Util.checkTableWritePermission(ds, tableName);
                } catch (ResourceNotFoundException ex) {
                    throw new IllegalArgumentException("table not found: " + tableName);
                }
                
                PluginFactory pf = new PluginFactory();
                TapSchemaDAO ts = pf.getTapSchemaDAO();
                ts.setDataSource(ds);
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

                DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
                try {
                    tm.startTransaction();

                    // create index
                    TableCreator tc = new TableCreator(ds);
                    tc.createIndex(cd, unique);

                    // update tap_schema
                    cd.indexed = true;
                    ts.put(td);

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
                    throw new RuntimeException("failed to update table " + tableName, ex);
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

                ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, new Date());
                logInfo.setSuccess(true);
            } catch (AccessControlException ex) {
                logInfo.setMessage(ex.getMessage());
                logInfo.setSuccess(true);
                ErrorSummary es = new ErrorSummary(ex.getMessage(), ErrorType.FATAL);
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, es, new Date());
            } catch (IllegalArgumentException ex) {
                logInfo.setMessage(ex.getMessage());
                logInfo.setSuccess(true);
                ErrorSummary es = new ErrorSummary(ex.getMessage(), ErrorType.FATAL);
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
        } finally {

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
