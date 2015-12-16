/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2014.                            (c) 2014.
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

package ca.nrc.cadc.sample;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AdqlQueryImplTest 
{
    private static final Logger log = Logger.getLogger(AdqlQueryImplTest.class);
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.sample", Level.INFO);
                
    }
    
    Job job = new Job()
    {
        @Override
        public String getID() { return "testJob"; }
    };
    
    public AdqlQueryImplTest() { }

    // this test makes ure that the AllColumnsConverter in the base AdqlQuery 
    // class still operates (eg we called super.init() correctly)
    @Test
    public void testOrigConverters()
    {
        try
        {
            job.getParameterList().add(new Parameter("QUERY", "select * from test.foo as t"));
            
            AdqlQueryImpl q = new AdqlQueryImpl();
            q.setJob(job);
            q.setTapSchema(mockTapSchema());
            
            String sql = q.getSQL();
            log.debug("SQL: " + sql);
            Assert.assertNotNull("sql", sql);
            sql = sql.toLowerCase();
            int i = sql.indexOf("select") + 6;
            int j = sql.indexOf("from") - 1;
            String selectList = sql.substring(i, j);
            log.debug("select-list: " + selectList);
            Assert.assertTrue("f1", selectList.contains("t.f1"));
            Assert.assertTrue("f1", selectList.contains("t.f2"));
            Assert.assertFalse("f1", selectList.contains("*"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }
    
    // this test requires that AdqlQueryImpl converts TOP to LIMIT
    @Test
    public void testTopConverter()
    {
        try
        {
            job.getParameterList().add(new Parameter("QUERY", "select top 5 * from test.foo"));
            
            AdqlQueryImpl q = new AdqlQueryImpl();
            q.setJob(job);
            q.setTapSchema(mockTapSchema());
            
            String sql = q.getSQL();
            log.debug("SQL: " + sql);
            Assert.assertNotNull("sql", sql);
            Assert.assertTrue("limit", sql.toLowerCase().endsWith("limit 5"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }
    
    TapSchema mockTapSchema()
    {
        TapSchema ret = new TapSchema();
        SchemaDesc sd = new SchemaDesc("test", null, null);
        TableDesc foo = new TableDesc("test", "test.foo", null, null);
        TableDesc bar = new TableDesc("test", "test.bar", null, null);
        foo.getColumnDescs().add(new ColumnDesc("test.foo", "f1", null, null, null, null, "adql:INTEGER", null));
        foo.getColumnDescs().add(new ColumnDesc("test.foo", "f2", null, null, null, null, "adql:CHAR", 8));
        bar.getColumnDescs().add(new ColumnDesc("test.bar", "b1", null, null, null, null, "adql:INTEGER", null));
        bar.getColumnDescs().add(new ColumnDesc("test.bar", "b2", null, null, null, null, "adql:CHAR", 8));
        sd.getTableDescs().add(foo);
        sd.getTableDescs().add(bar);
        ret.getSchemaDescs().add(sd);
        return ret;
    }
}
