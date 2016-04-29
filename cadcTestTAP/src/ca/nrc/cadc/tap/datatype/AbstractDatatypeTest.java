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

package ca.nrc.cadc.tap.datatype;

import ca.nrc.cadc.conformance.uws.ResultsTest;
import ca.nrc.cadc.xml.XmlUtil;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebResponse;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.filter.ElementFilter;
import static org.junit.Assert.*;
import org.junit.Before;

public abstract class AbstractDatatypeTest extends ResultsTest
{
    private static Logger log = Logger.getLogger(AbstractDatatypeTest.class);
    
    private static final String VOTABLE_SCHEMA_RESOURCE = "VOTable-v1.2.xsd";

    private static String votableSchema;

    public AbstractDatatypeTest()
    {
        super();
    }

    protected void setLoggingLevel(Logger log) { }

    @Before
    public void before()
    {
        super.before();
        
        // Get an URL to the VOTable schema in the jar.
        URL url = AbstractDatatypeTest.class.getClassLoader().getResource(VOTABLE_SCHEMA_RESOURCE);
        if (url == null)
            throw new MissingResourceException("Resource not found: " + VOTABLE_SCHEMA_RESOURCE,
                                               "TestTest", VOTABLE_SCHEMA_RESOURCE);
        votableSchema = url.toString();
        log.debug("VOTable schema url: " + votableSchema);
    }

    protected void validateResults(List<URL> resultUrls)
    {
        try
        {
            for (URL url : resultUrls)
            {
                // Download the url.
                WebConversation conversation = new WebConversation();
                WebResponse response = get(conversation, url.toString(), "application/x-votable+xml");
                // TODO: content-type is for VOTable

                // Validate the XML against the schema and get a JDOM Document.
                log.debug("XML:\r\n" + response.getText());
                Document document = XmlUtil.buildDocument(response.getText());

                // Get the table data with the query result.
                Element root = document.getRootElement();
                assertNotNull("XML returned from GET of " + url.toString() + " missing root element", root);
                Namespace namespace = root.getNamespace();

                // Iterator of TR elements.
                Iterator it = root.getDescendants(new ElementFilter("TR", namespace));
                assertTrue("No TR elements containing query results found", it.hasNext());

                // Get the first TR Element.
                Element tr = (Element) it.next();

                // List of TD elements.
                List list = tr.getChildren("TD", namespace);
                assertFalse("No TD elements containing query results found", list.isEmpty());
                assertEquals("Query should only return a single column of data", 1, list.size());

                // Validate the query result.
                Element td = (Element) list.get(0);
                if (td.getText() == null || td.getText().trim().length() == 0)
                    fail(this.getClass().getSimpleName() + ": null or zero length value");
                validateResult(td.getText());
            }
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    protected void validateResult(String value) {}
    
}
