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

import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.db.BasicDataTypeMapper;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Create table. This action creates a new database table and adds a description
 * to the tap_schema.
 * 
 * @author pdowler
 */
public class PutAction extends TablesAction {
    private static final Logger log = Logger.getLogger(PutAction.class);
    
    private static final String INPUT_TAG = "inputTable";

    public PutAction() { 
    }

    @Override
    public void doAction() throws Exception {
        String tableName = getTableName();
        String schemaName = getSchemaFromTable(tableName);
        log.debug("PUT: " + tableName);
        
        checkSchemaWritePermission(schemaName);
        
        TableDesc inputTable = getInputTable(schemaName, tableName);
        if (inputTable == null) {
            throw new IllegalArgumentException("no input table");
        }
        
        DataSource ds = getDataSource();
        DatabaseTransactionManager tm = new DatabaseTransactionManager(ds);
        try {
            TapSchemaDAO ts = new TapSchemaDAO();
            ts.setDataSource(ds);
            TableDesc td = ts.getTable(tableName);
            if (td != null) {
                throw new ResourceAlreadyExistsException("table " + tableName + " already exists");
            }
            
            tm.startTransaction();

            // create table
            TableCreator tc = new TableCreator(ds);
            tc.createTable(inputTable);
            
            // add to tap_schema
            ts.put(inputTable);
            
            tm.commitTransaction();
        } catch (Exception ex) {
            try {
                log.error("PUT failed - rollback", ex);
                tm.rollbackTransaction();
                log.error("PUT failed - rollback: OK");
            } catch (Exception oops) {
                log.error("PUT failed - rollback : FAIL", oops);
            }
            // TODO: categorise failures better
            throw new RuntimeException("failed to create/add " + tableName, ex);
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
        syncOutput.setCode(200);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new TableDescHandler(INPUT_TAG);
    }
    
    private TableDesc getInputTable(String schemaName, String tableName) {
        TableDesc input = (TableDesc) syncInput.getContent(INPUT_TAG);
        if (input == null) {
            throw new IllegalArgumentException("no input: expected a document describing the table to create");
        }
        
        input.setSchemaName(schemaName);
        input.setTableName(tableName);
        for (ColumnDesc cd : input.getColumnDescs()) {
            cd.setTableName(tableName);
        }
        
        return input;
    }
}
