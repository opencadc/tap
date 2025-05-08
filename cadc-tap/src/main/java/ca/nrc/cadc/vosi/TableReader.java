/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.vosi;

import ca.nrc.cadc.reg.XMLConstants;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.KeyColumnDesc;
import ca.nrc.cadc.tap.schema.KeyDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;

/**
 *
 * @author pdowler
 */
public class TableReader extends TableSetParser {

    private static final Logger log = Logger.getLogger(TableReader.class);

    private static final String TAP_TYPE = "TAPType";
    private static final String VOT_TYPE = "VOTableType";

    public TableReader() {
        this(true);
    }

    public TableReader(boolean enableSchemaValidation) {
        super(enableSchemaValidation);
    }

    public TableDesc read(InputStream istream)
            throws IOException, InvalidTableSetException {
        return read(new InputStreamReader(istream));
    }

    public TableDesc read(Reader reader)
            throws IOException, InvalidTableSetException {
        try {
            Document doc = parse(reader);
            Element root = doc.getRootElement();
            Namespace xsi = root.getNamespace("xsi");
            return toTable("default", root, xsi);
        } catch (JDOMException ex) {
            throw new InvalidTableSetException("invalid content", ex);
        }
    }

    static TableDesc toTable(String schemaName, Element te, Namespace xsi) {
        String tapType = TAP_TYPE;
        String votType = VOT_TYPE;
        for (Namespace ns : te.getNamespacesInScope()) {
            if (ns.getURI().equals(XMLConstants.VODATASERVICE_11_NS.toASCIIString())) {
                tapType = ns.getPrefix() + ":" + TAP_TYPE;
                votType = ns.getPrefix() + ":" + VOT_TYPE;
                log.debug("found: " + tapType + " " + votType);
                break;
            }
        }
        String tn = te.getChildTextTrim("name");
        TableDesc td = new TableDesc(schemaName, tn);
        td.description = te.getChildTextTrim("description");
        List<Element> cols = te.getChildren("column");
        for (Element ce : cols) {
            String cn = ce.getChildTextTrim("name");
            Element dte = ce.getChild("dataType");
            String dtt = dte.getAttributeValue("type", xsi);
            String dtv = dte.getTextTrim();
            String xtype = dte.getAttributeValue("extendedType");
            log.debug(cn + ": " + dtt + " " + dtv + " " + xtype);
            String arraysize = null;
            if (tapType.equals(dtt)) {
                dtv = "adql:" + dtv;
                String sz = dte.getAttributeValue("size");
                if (sz != null) {
                    arraysize = sz;
                }
                if (dtv.startsWith("adql:VAR")) {
                    if (arraysize == null) {
                        arraysize = "*";
                    } else {
                        arraysize += "*";
                    }
                } else if (dtv.equalsIgnoreCase("adql:REGION")) {
                    arraysize = "*";
                }
            } else if (votType.equals(dtt)) {
                arraysize = dte.getAttributeValue("arraysize");
            }

            TapDataType tt = new TapDataType(dtv, arraysize, xtype);
            log.debug("created: " + cn + " " + tt);
            ColumnDesc cd = new ColumnDesc(tn, cn, tt);
            cd.description = ce.getChildTextTrim("description");
            cd.ucd = ce.getChildTextTrim("ucd");
            cd.unit = ce.getChildTextTrim("unit");
            cd.utype = ce.getChildTextTrim("utype");
            //cd.indexed = "indexed".equals(ce.getChildTextTrim("flag"));
            cd.columnID = ce.getAttributeValue("columnID", TableSet.vte);
            td.getColumnDescs().add(cd);
        }

        List<Element> keys = te.getChildren("foreignKey");
        int i = 1;
        for (Element fk : keys) {
            String keyID = tn + "_key" + i;
            String tt = fk.getChildTextTrim("targetTable");
            KeyDesc kd = new KeyDesc(keyID, tn, tt);
            List<Element> fkcols = fk.getChildren("fkColumn");
            for (Element fkc : fkcols) {
                String fc = fkc.getChildTextTrim("fromColumn");
                String tc = fkc.getChildTextTrim("targetColumn");
                KeyColumnDesc kcd = new KeyColumnDesc(keyID, fc, tc);
                kd.getKeyColumnDescs().add(kcd);
            }
            td.getKeyDescs().add(kd);
        }

        return td;
    }
}
