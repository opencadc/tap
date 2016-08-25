package ca.nrc.cadc.tap.datatype;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.util.Log4jInit;
import java.util.List;
import org.apache.log4j.Level;
import org.jdom2.Attribute;
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
        Log4jInit.setLevel("ca.nrc.cadc.tap.datatype", Level.INFO);
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
    protected void validateQueryStatus(String filename, VOTableDocument document, String status)
    {
        VOTableResource resource = document.getResourceByType("results");
        Assert.assertNotNull(filename, resource);
        
        String qs = null;
        List<VOTableInfo> infos = resource.getInfos();
        for (VOTableInfo i : infos)
        {
            if ( "QUERY_STATUS".equals(i.getName()) )
                qs = i.getValue();
        }
        Assert.assertNotNull(filename + " query status", qs);
        Assert.assertEquals(filename + " query status", status, qs);
    }

}
