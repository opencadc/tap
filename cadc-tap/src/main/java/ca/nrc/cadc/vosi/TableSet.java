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
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.vosi;

import ca.nrc.cadc.reg.XMLConstants;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.KeyColumnDesc;
import ca.nrc.cadc.tap.schema.KeyDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.xml.W3CConstants;
import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;

/**
 * Class to convert the TapSchema content into a TableSet document.
 *
 * @author pdowler
 * @deprecated see TableSetWriter
 */
public class TableSet {
    private static final Logger log = Logger.getLogger(TableSet.class);

    private static final String ADQL_PREFIX = "adql";
    private static final String VOTABLE_PREFIX = "votable";

    // the default database schema -- not xml schema
    private static final String DEFAULT_SCHEMA = "default";

    private TapSchema tapSchema;

    private final Namespace xsi = W3CConstants.XSI_NS;
    private final Namespace vosi = VOSI.TABLES_NS;
    private final Namespace vod = XMLConstants.VODATASERVICE_NS;

    // move to cadc-vosi VOSI
    public static final String VTE_PREFIX = "vte";
    public static final String VTE_XSD = "VOSITables-ext-v1.0.xsd";
    public static final String VTE_NS_URI = "http://www.opencadc.org/xml/VOSITables-ext/v0.1";
    public static final Namespace vte = Namespace.getNamespace(VTE_PREFIX, VTE_NS_URI);

    public TableSet(TapSchema tapSchema) {
        this.tapSchema = tapSchema;
    }

    public Document getTableDocument(TableDesc td) {
        if (td == null) {
            throw new IllegalArgumentException("tableName cannot be null for Table document");
        }
        Element root = toXmlElement(td, vosi);
        return getDocument(root);
    }

    /**
     * Get TableSet document with all content.
     *
     * @return
     */
    public Document getDocument() {
        Element root = toXmlElement(tapSchema);
        return getDocument(root);
    }

    /**
     * Get a single Table document.
     *
     * @param root root element for document
     * @return the TapSchema as a document to be rendered as XML
     */
    protected Document getDocument(Element root) {
        root.addNamespaceDeclaration(xsi);
        root.addNamespaceDeclaration(vod);
        root.addNamespaceDeclaration(vte);

        // ivoa convention but not allowed by the VODataService schema
        //root.setAttribute("version", "1.1");
        Document document = new Document();
        document.addContent(root);
        return document;
    }

    /**
     * @param ts
     * @return
     */
    private Element toXmlElement(TapSchema ts) {
        if (ts.getSchemaDescs().isEmpty()) {
            throw new IllegalArgumentException("Error: at least one schema is required.");
        }

        Element eleTableset = new Element("tableset", vosi);
        for (SchemaDesc sd : ts.getSchemaDescs()) {
            eleTableset.addContent(toXmlElement(sd, Namespace.NO_NAMESPACE));
        }
        return eleTableset;
    }

    /**
     * @param sd
     * @return
     */
    private Element toXmlElement(SchemaDesc sd, Namespace ns) {
        Element ret = new Element("schema", ns);
        Element ele;
        ele = new Element("name");
        if (sd.getSchemaName() == null) {
            ele.setText(DEFAULT_SCHEMA);
        } else {
            ele.setText(sd.getSchemaName());
        }
        ret.addContent(ele);
        if (sd.description != null) {
            ele = new Element("description");
            ele.setText(sd.description);
            ret.addContent(ele);
        }
        if (sd.utype != null) {
            ele = new Element("utype");
            ele.setText(sd.utype);
            ret.addContent(ele);
        }
        if (sd.getTableDescs() != null) {
            for (TableDesc td : sd.getTableDescs()) {
                ret.addContent(toXmlElement(td, Namespace.NO_NAMESPACE));
            }
        }
        return ret;
    }

    /**
     * @param td
     * @return
     */
    private Element toXmlElement(TableDesc td, Namespace ns) {
        Element eleTable = new Element("table", ns);
        eleTable.setAttribute("type", "output");

        Element ele;
        addChild(eleTable, "name", td.getTableName());
        addChild(eleTable, "description", td.description);

        if (td.getColumnDescs() != null) {
            for (ColumnDesc cd : td.getColumnDescs()) {
                Element e = toXmlElement(cd);
                if (e != null) {
                    eleTable.addContent(e);
                }
            }
        }
        if (td.getKeyDescs() != null) {
            for (KeyDesc kd : td.getKeyDescs()) {
                Element e = toXmlElement(kd);
                if (e != null) {
                    eleTable.addContent(e);
                }
            }
        }
        return eleTable;
    }

    /**
     * @param cd
     * @return
     */
    private Element toXmlElement(ColumnDesc cd) {
        Element eleColumn = new Element("column");
        addChild(eleColumn, "name", cd.getColumnName());
        addChild(eleColumn, "description", cd.description);
        addChild(eleColumn, "unit", cd.unit);
        addChild(eleColumn, "ucd", cd.ucd);
        addChild(eleColumn, "utype", cd.utype);

        TapDataType tt = cd.getDatatype();

        String[] parts = tt.getDatatype().split(":");
        // unprefixed datatype is a VOTable type by default
        if (parts.length == 1 || isVOTableType(parts)) {
            Element eleDt = addChild(eleColumn, "dataType", tt.getDatatype());
            if (eleDt != null) {
                Attribute attType = new Attribute("type", vod.getPrefix() + ":VOTableType", xsi);
                eleDt.setAttribute(attType);
                if (tt.arraysize != null) {
                    eleDt.setAttribute("arraysize", tt.arraysize);
                }
                if (tt.xtype != null) {
                    eleDt.setAttribute("extendedType", tt.xtype);
                }
            }
        } else if (isTapType(parts)) { // backwards compatibility for TAP-1.0
            Element eleDt = addChild(eleColumn, "dataType", parts[1]);
            if (eleDt != null) {
                Attribute attType = new Attribute("type", vod.getPrefix() + ":TAPType", xsi);
                eleDt.setAttribute(attType);
                if (tt.arraysize != null && !tt.isVarSize()) {
                    eleDt.setAttribute("size", tt.arraysize);
                }
            }
        } else { // custom type
            log.warn("cannot convert " + cd + " to a legal VODataService column element, skipping");
            return null;
        }
        if (cd.indexed) {
            addChild(eleColumn, "flag", "indexed");
        }
        if (cd.columnID != null) {
            eleColumn.setAttribute(new Attribute("columnID", cd.columnID, vte));
        }

        return eleColumn;
    }

    private Element toXmlElement(KeyDesc kd) {
        Element ret = new Element("foreignKey");
        addChild(ret, "targetTable", kd.getTargetTable());
        for (KeyColumnDesc kc : kd.getKeyColumnDescs()) {
            Element fkc = new Element("fkColumn");
            addChild(fkc, "fromColumn", kc.getFromColumn());
            addChild(fkc, "targetColumn", kc.getTargetColumn());
            ret.addContent(fkc);
        }
        addChild(ret, "description", kd.description);
        addChild(ret, "utype", kd.utype);
        return ret;
    }

    private boolean isTapType(String[] parts) {

        if (parts.length == 2 && ADQL_PREFIX.equalsIgnoreCase(parts[0])) {
            return true;
        }
        return false;
    }

    private boolean isVOTableType(String[] parts) {
        if (parts.length == 2 && VOTABLE_PREFIX.equalsIgnoreCase(parts[0])) {
            return true;
        }
        return false;
    }

    private Element addChild(Element eleParent, String chdName, String chdText) {
        Element ele = null;
        if (chdText != null && !chdText.equals("")) {
            ele = new Element(chdName);
            ele.setText(chdText);
            eleParent.addContent(ele);
        }
        return ele;
    }

    public TapSchema getTapSchema() {
        return tapSchema;
    }

    public void setTapSchema(TapSchema tapSchema) {
        this.tapSchema = tapSchema;
    }
}
