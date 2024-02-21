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

package org.opencadc.youcat.tap;

import org.opencadc.youcat.tap.AdqlQueryImpl;
import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
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
public class AdqlQueryImplTest {

    private static final Logger log = Logger.getLogger(AdqlQueryImplTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.cat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.reg", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.util", Level.DEBUG);
    }

    public AdqlQueryImplTest() {
    }

    @Test
    public void testTOP() {
        System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "build/resources/test/config");
        try {
            String adql = "select TOP 5 foo from test.SomeTable";

            Job job = new Job() {
                public String getID() {
                    return "testJob";
                }
            };
            job.getParameterList().add(new Parameter("QUERY", adql));

            TapSchema tapSchema = loadTapSchema();

            AdqlQuery q = new AdqlQueryImpl();
            q.setJob(job);
            q.setTapSchema(tapSchema);

            String sql = q.getSQL();
            Assert.assertNotNull(sql);
            log.info("testTOP SQL: " + sql);
            sql = sql.toLowerCase();

            int i = sql.indexOf("limit");
            Assert.assertTrue("found limit", (i > 0));

            String limit = sql.substring(i).trim();
            log.info("testTOP: " + limit);

            Assert.assertEquals("limit 5", limit);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
        }
    }

    @Test
    public void testRegionConverterEnabled() {
        System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "build/resources/test/config");
        try {
            String adql = "select * from test.SomeTable"
                    + " where CONTAINS(pos, CIRCLE(NULL, 12.0, 34.0, 0.2)) = 1";

            Job job = new Job() {
                public String getID() {
                    return "testJob";
                }
            };
            job.getParameterList().add(new Parameter("QUERY", adql));

            TapSchema tapSchema = loadTapSchema();

            AdqlQuery q = new AdqlQueryImpl();
            q.setJob(job);
            q.setTapSchema(tapSchema);

            String sql = q.getSQL();
            Assert.assertNotNull(sql);
            log.info("testRegionConverterEnabled SQL: " + sql);
            sql = sql.toLowerCase();

            int i = sql.indexOf("where");
            Assert.assertTrue("found where", (i > 0));

            String where = sql.substring(i);
            log.info("testRegionConverterEnabled: " + where);

            Assert.assertTrue(where.contains("pos <@ scircle(spoint(radians(12.0), radians(34.0)), radians(0.2))"));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
        }
    }

    private TapSchema loadTapSchema() {
        TapSchema ret = new TapSchema();

        SchemaDesc sd = new SchemaDesc("test");
        TableDesc td = new TableDesc("test", "test.SomeTable");
        td.getColumnDescs().add(new ColumnDesc(td.getTableName(), "foo", TapDataType.STRING));
        td.getColumnDescs().add(new ColumnDesc(td.getTableName(), "pos", TapDataType.POINT));
        sd.getTableDescs().add(td);
        ret.getSchemaDescs().add(sd);
        
        ret.getFunctionDescs().add(new FunctionDesc("interval", TapDataType.INTERVAL));
        ret.getFunctionDescs().add(new FunctionDesc("point", TapDataType.POINT));
        ret.getFunctionDescs().add(new FunctionDesc("circle", TapDataType.CIRCLE));
        ret.getFunctionDescs().add(new FunctionDesc("polygon", TapDataType.POLYGON));
        
        ret.getFunctionDescs().add(new FunctionDesc("contains", TapDataType.INTEGER));
        ret.getFunctionDescs().add(new FunctionDesc("intersects", TapDataType.INTEGER));
        ret.getFunctionDescs().add(new FunctionDesc("coord1", TapDataType.DOUBLE));
        ret.getFunctionDescs().add(new FunctionDesc("coord2", TapDataType.DOUBLE));
        
        return ret;
    }
}
