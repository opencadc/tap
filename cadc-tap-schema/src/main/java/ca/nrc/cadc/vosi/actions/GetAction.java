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

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.schema.TapSchemaLoader;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import ca.nrc.cadc.vosi.TableSetWriter;
import ca.nrc.cadc.vosi.TableWriter;
import java.io.OutputStreamWriter;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class GetAction extends TablesAction {

    private static final Logger log = Logger.getLogger(GetAction.class);

    public GetAction() {
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
        log.debug("GET: " + schemaName + " " + tableName);
        
        checkReadable();

        final String detail = syncInput.getParameter("detail");
        int depth = TapSchemaDAO.MIN_DEPTH;
        // TODO: default depth used to be configurable... worth it?
        if (tableName == null && schemaName == null) {
            depth = TapSchemaDAO.TAB_DEPTH; // VOSI-tables-1.1 tableset
            if ("min".equalsIgnoreCase(detail)) {
                depth = TapSchemaDAO.TAB_DEPTH;
            } else if ("max".equalsIgnoreCase(detail)) {
                depth = TapSchemaDAO.MAX_DEPTH;
            } else if (detail != null) {
                throw new IllegalArgumentException("invalid parameter value detail=" + detail);
            }
        } else if (schemaName != null) {
            if ("tab".equalsIgnoreCase(detail)) {
                depth = TapSchemaDAO.TAB_DEPTH; // list tables
            }
        }

        TapSchemaDAO dao = getTapSchemaDAO();
        if (tableName != null) {
            checkTableReadPermissions(dao, tableName, logInfo);
            TableDesc td = dao.getTable(tableName);
            if (td == null) {
                // currently, permission check already threw this
                throw new ResourceNotFoundException("table not found: " + tableName);
            }

            // If the Accept header = application/x-votable+xml,
            // output the TableDesc as a VOTable
            String accept = syncInput.getHeader("Accept");
            if (VOTableWriter.CONTENT_TYPE.equals(accept)) {
                VOTableDocument vot = TapSchemaUtil.createVOTable(td);
                VOTableWriter tw = new VOTableWriter();
                syncOutput.setCode(200);
                syncOutput.setHeader("Content-Type", VOTableWriter.CONTENT_TYPE);
                tw.write(vot, new OutputStreamWriter(syncOutput.getOutputStream()));
            } else {
                TableWriter tw = new TableWriter();
                syncOutput.setCode(200);
                syncOutput.setHeader("Content-Type", "text/xml");
                tw.write(td, new OutputStreamWriter(syncOutput.getOutputStream()));
            }
        } else if (schemaName != null) {
            checkViewSchemaPermissions(dao, schemaName, logInfo);
            // TODO: TapSchemaDAO only supports schema only, ok for detail=min
            // should at least list tables for default detail
            // should provide columns at detail=max
            SchemaDesc sd = dao.getSchema(schemaName, depth);
            if (sd == null) {
                // currently, permission check already threw this
                throw new ResourceNotFoundException("schema not found: " + schemaName);
            }
            TapSchema tapSchema = new TapSchema();
            tapSchema.getSchemaDescs().add(sd);
            
            TableSetWriter tsw = new TableSetWriter();
            syncOutput.setCode(200);
            syncOutput.setHeader("Content-Type", "text/xml");
            tsw.write(tapSchema, new OutputStreamWriter(syncOutput.getOutputStream()));
        } else {
            TapSchemaLoader loader = new TapSchemaLoader(dao);
            TapSchema tapSchema = loader.load(depth);
            
            TableSetWriter tsw = new TableSetWriter();
            syncOutput.setCode(200);
            syncOutput.setHeader("Content-Type", "text/xml");
            tsw.write(tapSchema, new OutputStreamWriter(syncOutput.getOutputStream()));
        }
    }
}
