/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.tap.UploadManager;
import static ca.nrc.cadc.tap.UploadManager.SCHEMA;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import java.io.IOException;

/**
 * Implements the VOTableParser interface using JDOM.
 *
 * @author jburke
 */
public class JDOMVOTableParser implements VOTableParser
{
    private static final Logger log = Logger.getLogger(JDOMVOTableParser.class);

    protected UploadTable upload;
    
    protected String tableName;
    protected VOTableTable votable;
    protected UploadLimits uploadLimits;

    public JDOMVOTableParser() { }

    @Override
    public void setUpload(UploadTable upload, UploadLimits uploadLimits)
    {
        this.upload = upload;
        this.uploadLimits = uploadLimits;
    }

    private void init()
        throws IOException
    {
        if (votable == null)
        {
            verifyUploadTable();
            VOTableReader r = new VOTableReader();
            VOTableDocument doc = r.read(upload.uri.toURL().openStream());
            VOTableResource vr = doc.getResourceByType("results");

            this.votable = vr.getTable();

            this.tableName = upload.tableName;
            if (!tableName.toUpperCase().startsWith(SCHEMA)) {
                tableName = SCHEMA + "." + tableName;
            }
        }
    }

    void verifyUploadTable() throws IOException {
        if (uploadLimits != null) {
            final VOTableReader voTableReader = new VOTableReader();
            try (final ByteCountInputStream byteCountInputStream =
                         new ByteCountInputStream(upload.uri.toURL().openStream(), uploadLimits.getByteLimit())) {
                final VOTableDocument doc = voTableReader.read(byteCountInputStream);
                final VOTableResource vr = doc.getResourceByType("results");
                final VOTableTable voTableTable = vr.getTable();

                if (voTableTable.getFields().size() > uploadLimits.getColumnLimit()) {
                    throw new IOException("Column count exceeds maximum of " + uploadLimits.getColumnLimit());
                }

                int counter = 0;
                for (final Iterator<List<Object>> iterator = voTableTable.getTableData().iterator(); iterator.hasNext(); ) {
                    if (++counter > uploadLimits.getRowLimit()) {
                        throw new IOException("Row count exceeds maximum of " + uploadLimits.getRowLimit());
                    } else {
                        iterator.next();
                    }
                }
            }
        }
    }
    
    /**
     * Get a List that describes each VOTable column.
     *
     * @throws java.io.IOException if unable to parse the VOTable.
     * @return List of ColumnDesc describing the VOTable columns.
     */
    @Override
    public TableDesc getTableDesc()
        throws IOException
    {
        init();
        TableDesc tableDesc = TapSchemaUtil.createTableDesc(UploadManager.SCHEMA, tableName, votable);
        log.debug("table: " + tableDesc);
        return tableDesc;
    }

    /**
     * Returns an Iterator to the VOTable data.
     *
     * @return Iterator to the VOTable data.
     */
    @Override
    public Iterator<List<Object>> iterator()
    {
        if (votable != null && votable.getTableData() != null)
            return votable.getTableData().iterator();
        
        // return empty iterator
        return new ArrayList<List<Object>>().iterator();
    }
}
