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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.db.FitsTableData;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.io.IOException;
import java.io.InputStream;
import org.apache.log4j.Logger;
import org.opencadc.tap.io.AsciiTableData;
import org.opencadc.tap.io.TableDataInputStream;

/**
 * ContentHandler for SyncLoadAction.
 * 
 * @author pdowler
 */
public class TableContentHandler implements InlineContentHandler {
    
    private static final Logger log = Logger.getLogger(TableContentHandler.class);
    
    public static final String MSG = "message";
    public static final String CONTENT_TYPE_CSV = "text/csv";
    public static final String CONTENT_TYPE_TSV = "text/tab-separated-values";
    public static final String CONTENT_TYPE_FITS = "application/fits";
    
    private static final int BATCH_SIZE = 1000;
    
    private final SyncLoadAction parent;
    
    TableContentHandler(SyncLoadAction parent) {
        this.parent = parent;
    }

    @Override
    public Content accept(String name, String contentType, InputStream inputStream)
            throws InlineContentException, IOException, ResourceNotFoundException, TransientException {
        String tableName = parent.getTableName();
        log.debug("TableContentHandler: " + tableName);
        
        parent.checkWritableImpl();
        
        if (tableName == null) {
            throw new IllegalArgumentException("Missing table name in path");
        }
        if (contentType == null) {
            throw new IllegalArgumentException("Table Content-Type is required.");
        }

        TapSchemaDAO ts = parent.getTapSchemaDAO();
        parent.checkTableWritePermissions(ts, tableName);

        TableDesc targetTableDesc = ts.getTable(tableName);
        if (targetTableDesc == null) {
            throw new ResourceNotFoundException("Table not found: " + tableName);
        }

        log.debug("Content-Type: " + contentType);
        TableDataInputStream tableData = null;
        if (contentType.equals(CONTENT_TYPE_CSV) || contentType.equals(CONTENT_TYPE_TSV)) {
            tableData = new AsciiTableData(inputStream, contentType);
        }
        if (contentType.equals(CONTENT_TYPE_FITS)) {
            tableData = new FitsTableData(inputStream);
        }
        if (tableData == null) {       
            throw new IllegalArgumentException("Unsupported table ContentType: " + contentType);
        }
        
        TableLoader tl = new TableLoader(parent.getDataSource(), BATCH_SIZE);
        tl.load(targetTableDesc, tableData);

        String msg = "Inserted " + tl.getTotalInserts() + " rows to table " + tableName;
    
        InlineContentHandler.Content content = new InlineContentHandler.Content();
        content.name = MSG;
        content.value = msg;
        
        return content;
    }
}
