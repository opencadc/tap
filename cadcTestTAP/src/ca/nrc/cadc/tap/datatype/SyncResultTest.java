package ca.nrc.cadc.tap.datatype;

import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.junit.Assert;

/**
 *
 * @author jburke
 */
public class SyncResultTest extends AbstractSyncTest
{
    static
    {
        className = "SyncResultTest";
        Log4jInit.setLevel("ca.nrc.cadc.cat.integration", Level.INFO);
    }

    protected Element root;
    protected Namespace namespace;

    public SyncResultTest()
    {
        super();
        expectedQueryStatus = "OK";
        expectedResponseCode = 200;
    }

    @Override
    protected void validateQueryStatus(String filename, Document document, String status)
    {
        root = document.getRootElement();
        namespace = root.getNamespace();
        Assert.assertNotNull(filename, root);

        Element resource = root.getChild("RESOURCE", namespace);
        Assert.assertNotNull(filename, resource);

        Element info = resource.getChild("INFO", namespace);
        Assert.assertNotNull(filename, info);

        Attribute name = info.getAttribute("name");
        Assert.assertNotNull(filename, name);
        Assert.assertEquals(filename, "QUERY_STATUS", name.getValue());

        Attribute value = info.getAttribute("value");
        Assert.assertNotNull(filename, value);
        Assert.assertEquals(filename, status, value.getValue());
    }

}
