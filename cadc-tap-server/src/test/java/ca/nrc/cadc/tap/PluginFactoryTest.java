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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.writer.format.DefaultFormatFactory;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author pdowler
 */
public class PluginFactoryTest {
    private static final Logger log = Logger.getLogger(PluginFactoryTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
    }

    private static final Job JOB = new Job() {
        @Override
        public String getID() {
            return "abcdefg";
        }
    };

    public PluginFactoryTest() {
    }

    @Test
    @Ignore("Just a template")
    public void testTemplate() {
        try {
            // Test code.
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    @Test
    public void testSetup() {
        try {
            JOB.getParameterList().clear();
            JOB.getParameterList().add(new Parameter("LANG", "ADQL"));

            PluginFactory pf = new PluginFactory(JOB);

            try {
                Assert.assertNull(pf.getTapQuery()); // no default
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected exception: " + expected);
            }

            MaxRecValidator mrv = pf.getMaxRecValidator();
            Assert.assertNotNull(mrv);
            Assert.assertEquals(MaxRecValidator.class, mrv.getClass()); // default impl

            UploadManager um = pf.getUploadManager();
            Assert.assertNotNull(um);
            Assert.assertEquals(DefaultUploadManager.class, um.getClass()); // default impl

            TableWriter tw = pf.getTableWriter();
            Assert.assertNotNull(tw);
            Assert.assertEquals(DefaultTableWriter.class, tw.getClass()); // default impl

            FormatFactory ff = pf.getFormatFactory();
            Assert.assertNotNull(ff);
            Assert.assertEquals(DefaultFormatFactory.class, ff.getClass()); // default impl

            TapSchemaDAO tsd = pf.getTapSchemaDAO();
            Assert.assertNotNull(tsd);
            Assert.assertEquals(TapSchemaDAO.class, tsd.getClass()); // default impl
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    @Test
    public void getDefaultFormatFactory() {
        JOB.getParameterList().clear();
        JOB.getParameterList().add(new Parameter("PARAM1", "VALUE1"));

        try {
            final PluginFactory testSubject = new PluginFactory(JOB);

            final FormatFactory formatFactory = testSubject.getFormatFactory();
            Assert.assertEquals("Should be default factory.", formatFactory.getClass().getSimpleName(),
                                DefaultFormatFactory.class.getSimpleName());
        } catch (Exception e) {
            log.error("unexpected exception", e);
            throw e;
        }
    }

    @Test
    public void getTestFormatFactory() {
        JOB.getParameterList().clear();
        JOB.getParameterList().add(new Parameter("PARAM1", "VALUE1"));

        final List<FormatFactory> formatFactories = new ArrayList<>();

        formatFactories.add(new TestFormatFactory());
        formatFactories.add(new AnotherTestFormatFactory());

        try {
            final PluginFactory testSubject = new PluginFactory(JOB) {
                /**
                 * Pull the FormatFactory that is loaded, or the Default one if none found.
                 *
                 * @return FormatFactory instance.
                 */
                @Override
                FormatFactory loadFormatFactory() {
                    return this.selectFormatFactory(formatFactories);
                }
            };

            final FormatFactory formatFactory = testSubject.getFormatFactory();
            Assert.assertEquals("Should be test default factory.", formatFactory.getClass().getName(),
                                TestFormatFactory.class.getName());
        } catch (Exception e) {
            log.error("unexpected exception", e);
            throw e;
        }
    }

    @Test
    public void getAnotherTestFormatFactory() {
        JOB.getParameterList().clear();
        JOB.getParameterList().add(new Parameter("PARAM1", "VALUE1"));

        final List<FormatFactory> formatFactories = new ArrayList<>();

        formatFactories.add(new AnotherTestFormatFactory());
        formatFactories.add(new TestFormatFactory());

        try {
            final PluginFactory testSubject = new PluginFactory(JOB) {
                /**
                 * Pull the FormatFactory that is loaded, or the Default one if none found.
                 *
                 * @return FormatFactory instance.
                 */
                @Override
                FormatFactory loadFormatFactory() {
                    return this.selectFormatFactory(formatFactories);
                }
            };

            final FormatFactory formatFactory = testSubject.getFormatFactory();
            Assert.assertEquals("Should be test default factory.", formatFactory.getClass().getName(),
                                AnotherTestFormatFactory.class.getName());
        } catch (Exception e) {
            log.error("unexpected exception", e);
            throw e;
        }
    }

    public class TestFormatFactory implements FormatFactory {

        public TestFormatFactory() {
        }

        @Override
        public List<Format<Object>> getFormats(List<TapSelectItem> selectList) {
            return new ArrayList<>();
        }

        @Override
        public Format<Object> getFormat(TapSelectItem selectitem) {
            return new Format<Object>() {
                @Override
                public Object parse(String s) {
                    return s;
                }

                @Override
                public String format(Object o) {
                    return o.toString();
                }
            };
        }

        @Override
        public void setJob(Job job) {

        }
    }

    public class AnotherTestFormatFactory implements FormatFactory {

        public AnotherTestFormatFactory() {
        }

        @Override
        public List<Format<Object>> getFormats(List<TapSelectItem> selectList) {
            return new ArrayList<>();
        }

        @Override
        public Format<Object> getFormat(TapSelectItem selectitem) {
            return new Format<Object>() {
                @Override
                public Object parse(String s) {
                    return s;
                }

                @Override
                public String format(Object o) {
                    return o.toString();
                }
            };
        }

        @Override
        public void setJob(Job job) {

        }
    }
}
