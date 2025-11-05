package ca.nrc.cadc.tap.datatype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.conformance.uws.AbstractUWSTest;
import ca.nrc.cadc.conformance.uws.TestProperties;
import ca.nrc.cadc.conformance.uws.TestPropertiesList;
import ca.nrc.cadc.conformance.uws.Util;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.xml.XmlUtil;

import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import java.io.StringReader;

/**
 *
 * @author jburke
 */
public abstract class AbstractSyncTest extends AbstractUWSTest
{
    protected static final Logger log = Logger.getLogger(AbstractSyncTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.cat.test", Level.INFO);
    }

    protected static final String VOTABLE_NAMESPACE = "http://www.ivoa.net/xml/VOTable/v1.2";
    protected static final String VOTABLE_SCHEMA_RESOURCE = "VOTable-v1.2.xsd";
    protected static String votableSchema;
    protected static TestPropertiesList testPropertiesList;
    protected static String className;
    protected String expectedQueryStatus;
    protected int expectedResponseCode;

    public AbstractSyncTest()
    {
        super();
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // Get an URL to the VOTable schema in the jar.
        votableSchema = XmlUtil.getResourceUrlString(VOTABLE_SCHEMA_RESOURCE, AbstractSyncTest.class);
        if (votableSchema == null)
            Assert.fail("VOTable schema resource not found: " + VOTABLE_SCHEMA_RESOURCE);
        log.debug("VOTable schema url: " + votableSchema);

        String propertiesDirectory = System.getProperty("properties.directory");
        if (propertiesDirectory == null)
            Assert.fail("properties.directory System property not set");
        try
        {
            testPropertiesList = new TestPropertiesList(propertiesDirectory, className);
        }
        catch (IOException e)
        {
            log.error(e);
             Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFunctions()
    {
        String label = null;
        try
        {
            if (testPropertiesList.propertiesList.isEmpty())
                 Assert.fail("missing properties file for " + className);

            // For each properties file.
            for (TestProperties properties : testPropertiesList.propertiesList)
            {
                log.info("test properties file: " + properties.filename);
                label = properties.filename;
                log.debug(properties);

                // Create a new Job with UPLOAD parameters.
                WebConversation conversation = new WebConversation();
                conversation.setExceptionsThrownOnErrorStatus(false); // expect 400 with VOTable in body
                WebRequest postRequest = new PostMethodWebRequest(serviceUrl, true);
                postRequest.setParameter("runId", new String[] {"INTTEST"});

                if (properties.parameters != null)
                {
                    List<String> valueList;

                    List<String> keyList = new ArrayList<String>(properties.parameters.keySet());
                    for (String key : keyList)
                    {
                        valueList = properties.parameters.get(key);
                        String[] values = valueList.toArray(new String[0]);
                        postRequest.setParameter(key, values);
                        log.debug("parameter: " + key + "=" + values[0]);
                    }
                }

                log.debug(Util.getRequestParameters(postRequest));

                // POST the Job.
                WebResponse response = conversation.getResponse(postRequest);
                Assert.assertNotNull("POST response to " + serviceUrl + " is null", response);

                log.debug(Util.getResponseHeaders(response));

                int responseCode = response.getResponseCode();
                log.debug("Response code: " + responseCode);
                
                if (responseCode == 303)
                {
                    // Get the redirect.
                    String location = response.getHeaderField("Location");
                    log.debug("Location: " + location);
                    Assert.assertNotNull("POST response to " + serviceUrl + " location header not set", location);

                    // Get the job.
                    response = get(conversation, location, "application/x-votable+xml");
                    responseCode = response.getResponseCode();
                    
                    log.debug("Response code: " + responseCode);
                    log.debug(Util.getResponseHeaders(response));
                }

                Assert.assertEquals(properties.filename + ": response code", this.expectedResponseCode, responseCode);
                
                // Validate the XML against the schema and get a DOM Document.
                //log.debug("XML:\r\n" + response.getText());
                //Map<String, String> map = new HashMap<String, String>();
                //map.put(VOTABLE_NAMESPACE, votableSchema);
                //Document document = XmlUtil.buildDocument(new StringReader(response.getText()), map);
                //validateQueryStatus(properties.filename, document, expectedQueryStatus);
                VOTableReader vtr = new VOTableReader();
                VOTableDocument doc = vtr.read(response.getText());
                validateQueryStatus(properties.filename, doc, expectedQueryStatus);
            }
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
             Assert.fail(label + " unexpected exception: " + unexpected);
        }
    }

    protected abstract void validateQueryStatus(String filename, VOTableDocument document, String status) throws IOException;

}
