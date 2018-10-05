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

import java.io.InputStream;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.db.AsciiTableData;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;

public class PostAction extends TablesAction {
    
    private static final Logger log = Logger.getLogger(PostAction.class);
    
    private static final int BATCH_SIZE = 1000;
    
    public PostAction() {
    }

    @Override
    public void doAction() throws Exception {
        String tableName = getTableName();
        String schemaName = getSchemaFromTable(tableName);
        log.debug("POST: " + tableName);
        
        checkSchemaWritePermission(schemaName);
        
        DataSource ds = getDataSource();
        
        try {
            
            TapSchemaDAO ts = new TapSchemaDAO();
            ts.setDataSource(ds);
            TableDesc tableDesc = ts.getTable(tableName);
            if (tableDesc == null) {
                throw new ResourceNotFoundException("Table not found: " + tableName);
            }
            
            InputStream content = (InputStream) syncInput.getContent(TableContentHandler.TABLE_CONTENT);
            AsciiTableData tableData = new AsciiTableData(content, syncInput.getHeader("ContentType"), tableDesc);            
            TableLoader tl = new TableLoader(ds, BATCH_SIZE);
            tl.load(tableData.getTableDesc(), tableData);
            
            String msg = "Inserted " + tl.getTotalInserts() + " rows to table " + tableName;
            syncOutput.getOutputStream().write(msg.getBytes("UTF-8"));
            
        } catch (Exception ex) {
            log.error("POST failed: " + ex);
            throw new RuntimeException("failed to insert rows to table " + tableName, ex);
        }
        syncOutput.setCode(200);        
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new TableContentHandler();
    }

}