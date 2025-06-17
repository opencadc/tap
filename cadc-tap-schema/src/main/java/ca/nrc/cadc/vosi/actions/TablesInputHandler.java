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
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vosi.InvalidTableSetException;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.TableSetReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TablesInputHandler implements InlineContentHandler {
    private static final Logger log = Logger.getLogger(TablesInputHandler.class);

    private static final long BYTE_LIMIT = 1024 * 1024L; // 1 MiB
    public static final String VOSI_TABLE_TYPE = "text/xml";
    public static final String VOTABLE_TYPE = "application/x-votable+xml";
    public static final String VOSI_SCHEMA_TYPE = "application/x-vosi-schema";
    // VOSI tableset schema cannot carry owner information
    //public static final String VOSI_SCHEMA_TYPE = "text/plain"; // key = value
    
    private String objectTag;
    
    public TablesInputHandler(String objectTag) { 
        this.objectTag = objectTag;
    }

    @Override
    public Content accept(String name, String contentType, InputStream in) throws InlineContentException, IOException {
        log.debug("accept: " + name + " " + contentType);
        try {
            String schemaOwner = null;
            SchemaDesc sch = null;
            TableDesc tab = null;
            if (VOSI_SCHEMA_TYPE.equalsIgnoreCase(contentType)) {
                ByteCountInputStream istream = new ByteCountInputStream(in, BYTE_LIMIT);
                String str = StringUtil.readFromInputStream(istream, "UTF-8");
                log.debug("raw input:\n" + str);
                
                TableSetReader tsr = new TableSetReader(false);  // schema validation causes default arraysize="1" to be injected
                TapSchema ts = tsr.read(new StringReader(str));
                if (ts.getSchemaDescs().isEmpty() || ts.getSchemaDescs().size() > 1) {
                    throw new IllegalArgumentException("invalid input: expected 1 schema in " + VOSI_SCHEMA_TYPE
                        + " found: " + ts.getSchemaDescs().size());
                }
                sch = ts.getSchemaDescs().get(0);
            } else if (VOSI_TABLE_TYPE.equalsIgnoreCase(contentType)) {
                ByteCountInputStream istream = new ByteCountInputStream(in, BYTE_LIMIT);
                String xml = StringUtil.readFromInputStream(istream, "UTF-8");
                TableReader tr = new TableReader(false); // schema validation causes default arraysize="1" to be injected
                log.debug("raw input:\n" + xml);
                tab = tr.read(new StringReader(xml));
            } else if (VOTABLE_TYPE.equalsIgnoreCase(contentType)) {
                VOTableReader tr = new VOTableReader();
                ByteCountInputStream istream = new ByteCountInputStream(in, BYTE_LIMIT);
                VOTableDocument doc = tr.read(istream);
                tab = toTableDesc(doc);
            }
            InlineContentHandler.Content ret = new InlineContentHandler.Content();
            ret.name = objectTag;
            ret.value = tab;
            if (sch != null) {
                ret.value = sch;
            }
            return ret;
        } catch (InvalidTableSetException ex) {
            throw new IllegalArgumentException("invalid input document", ex);
        }
    }
    
    private TableDesc toTableDesc(VOTableDocument doc) {
        // TODO: reject if the table has any rows? try to insert them if it is small enough?
        for (VOTableResource vr : doc.getResources()) {
            VOTableTable vtab = vr.getTable();
            if (vtab != null) {
                TableDesc ret = TapSchemaUtil.createTableDesc("default", "default", vtab);
                log.debug("create from VOtable: " + ret);
                // strip out some incoming table metadata
                // - ID attr (should be transient usage only)
                for (ColumnDesc cd : ret.getColumnDescs()) {
                    cd.columnID = null;
                }
                return ret;
            }
        }
        throw new IllegalArgumentException("no table description found in VOTable document");
    }
}
