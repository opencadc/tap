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
************************************************************************
*/

package ca.nrc.cadc.vosi.actions;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Update schema or table metadata.
 * @author pdowler
 */
public class PostAction extends TablesAction {
    private static final Logger log = Logger.getLogger(PostAction.class);

    public PostAction() { 
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new TablesInputHandler(INPUT_TAG);
    }

    @Override
    public void doAction() throws Exception {
        String schemaName = null;
        String tableName = null;
        String[] target = getTarget();
        if (target != null) {
            schemaName = target[0];
            tableName = target[1];
        }
        log.debug("target: " + schemaName + " " + tableName);
        
        checkWritable();
        
        if (schemaName == null && tableName == null) {
            throw new IllegalArgumentException("missing schema|table name in path");
        }
        
        TapSchemaDAO ts = getTapSchemaDAO();
        if (tableName != null) {
            TablesAction.checkTableWritePermissions(ts, tableName, logInfo);
            updateTable(ts, schemaName, tableName);
        } else {
            TablesAction.checkSchemaWritePermissions(ts, schemaName, logInfo);
            updateSchema(ts, schemaName);
        }
        
        syncOutput.setCode(204); // no content on success
    }
    
    private void updateTable(TapSchemaDAO dao, String schemaName, String tableName) 
            throws ResourceNotFoundException {
        TableDesc inputTable = getInputTable(schemaName, tableName);
        if (inputTable == null) {
            throw new IllegalArgumentException("no input table");
        }
        
        TableDesc cur = dao.getTable(tableName);
        if (cur == null) {
            throw new ResourceNotFoundException("not found: table " + tableName);
        }
        
        TapSchemaDAO.checkMismatchedColumnSet(cur, inputTable);

        // merge allowed changes
        int numCols = 0;
        cur.description = inputTable.description;
        cur.utype = inputTable.utype;
        for (ColumnDesc cd : cur.getColumnDescs()) {
            ColumnDesc inputCD = inputTable.getColumn(cd.getColumnName());
            // above column match check should catch this, but just in case:
            if (inputCD == null) {
                throw new IllegalArgumentException("column missing from input table: " + cd.getColumnName());
            }
            if (!cd.getDatatype().equals(inputCD.getDatatype())) {
                throw new UnsupportedOperationException("cannot change " + cd.getColumnName() + " from "
                        + cd.getDatatype() + " -> " + inputCD.getDatatype());
            }
            cd.description = inputCD.description;
            cd.ucd = inputCD.ucd;
            cd.unit = inputCD.unit;
            cd.utype = inputCD.utype;
            cd.principal = inputCD.principal;
            cd.std = inputCD.std;
            cd.columnID = inputCD.columnID;
            // ignore: indexed (internal)
            numCols++;
        }
        if (numCols != cur.getColumnDescs().size()) {
            throw new IllegalArgumentException("column list mismatch: cannot update");
        }
        // update
        dao.put(cur);
    }
    
    private void updateSchema(TapSchemaDAO dao, String schemaName) 
            throws ResourceNotFoundException {
        SchemaDesc inputSchema = getInputSchema(schemaName);
        
        SchemaDesc cur = dao.getSchema(schemaName, 0);
        if (cur == null) {
            throw new ResourceNotFoundException("not found: schema " + schemaName);
        }
        // merge allowed changes
        cur.description = inputSchema.description;
        cur.utype = inputSchema.utype;
        // update
        dao.put(cur);
    }
}
