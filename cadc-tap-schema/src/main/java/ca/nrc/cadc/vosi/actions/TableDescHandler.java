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


import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.vosi.InvalidTableSetException;
import ca.nrc.cadc.vosi.TableReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TableDescHandler implements InlineContentHandler {
    private static final Logger log = Logger.getLogger(TableDescHandler.class);

    private static final long BYTE_LIMIT = 1024 * 1024L; // 1 MiB
    private static final String VOSI_TABLE_TYPE = "text/xml";
    private static final String VOTABLE_TYPE = "application/x-votable+xml";
    
    private String objectTag;
    
    public TableDescHandler(String objectTag) { 
        this.objectTag = objectTag;
    }

    @Override
    public Content accept(String name, String contentType, InputStream in) throws InlineContentException, IOException {
        try {
            List<ColumnDesc> cols = null;
            if (VOSI_TABLE_TYPE.equalsIgnoreCase(contentType)) {
                TableReader tr = new TableReader();
                ByteCountInputStream istream = new ByteCountInputStream(in, BYTE_LIMIT);
                TableDesc td = tr.read(istream);
                cols = td.getColumnDescs();
            } else if (VOTABLE_TYPE.equalsIgnoreCase(contentType)) {
                VOTableReader tr = new VOTableReader();
                ByteCountInputStream istream = new ByteCountInputStream(in, BYTE_LIMIT);
                VOTableDocument doc = tr.read(istream);
                cols = toTableDesc(doc);
            }
            InlineContentHandler.Content ret = new InlineContentHandler.Content();
            ret.name = objectTag;
            ret.value = cols;
            return ret;
        } catch (InvalidTableSetException ex) {
            throw new IllegalArgumentException("invalid input document", ex);
        }
    }
    
    private List<ColumnDesc> toTableDesc(VOTableDocument doc) {
        for (VOTableResource vr : doc.getResources()) {
            VOTableTable vtab = vr.getTable();
            if (vtab != null) {
                List<ColumnDesc> ret = new ArrayList<ColumnDesc>();
                int col = 0;
                for (VOTableField f : vtab.getFields()) {
                    TapDataType dt = new TapDataType(f.getDatatype(), f.getArraysize(), f.xtype);
                    ColumnDesc cd = new ColumnDesc(null, f.getName(), dt);
                    cd.description = f.description;
                    cd.id = f.id;
                    cd.ucd = f.ucd;
                    cd.unit = f.unit;
                    cd.utype = f.utype;
                    cd.column_index = col++; // preserve order
                    ret.add(cd);
                }
            }
        }
        throw new UnsupportedOperationException("votable to tap_schema");
    }
}
