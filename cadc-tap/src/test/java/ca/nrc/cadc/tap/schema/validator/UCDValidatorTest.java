/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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

package ca.nrc.cadc.tap.schema.validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import ca.nrc.cadc.tap.schema.validator.ucd.UCDValidator;
import ca.nrc.cadc.tap.schema.validator.ucd.UCDVocabulary;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class UCDValidatorTest {

    // This test is to ensure that the UCDVocabulary is initialized properly.
    @Test
    public void UCDVocabularyInitiationTest() {
        assertFalse(UCDVocabulary.getAllWords().isEmpty());
    }

    @Test
    public void testUCD() {
        UCDValidator validator = new UCDValidator(ValidatorConfig.strict());

        List<String> validUCDs = List.of(
                "phot.flux",
                "arith.grad;em.UV", // P, S
                "arith.rate;em.IR.3-4um", // P, s
                "arith.factor;arith.diff", // P, S
                "phot.flux;phot.mag", // E,E
                "em.wl;em.UV", // Q,S
                "stat.error;phot.mag;em.opt.B", // P, E, S
                "phot;phot.color" // E, C
        );
        List<String> invalidUCDs = List.of("", " ", ";",
                "abc", // unknown
                "em.UV;arith.grad", // S, P
                "arith.rate;arith.factor", // P, P - Primary-only word in the secondary position
                "em.radio", // S - secondary-only word in the primary position
                "em.IR.K.Brgamma" // Deprecated word
        );

        for (String ucd : validUCDs) {
            assertTrue(ucd + " failed to pass.", validator.validate(ucd).isValid());
        }

        for (String ucd : invalidUCDs) {
            assertFalse(ucd + " failed to fail.", validator.validate(ucd).isValid());
        }
    }

    // TODO: This test has to be Commented unless testing
    //@Test
    public void testUCDFile() throws IOException, NoSuchAlgorithmException {
        String ivoaResource = "https://www.ivoa.net/Documents/UCD1+/20241218/ucd-list.txt";
        String localResource = "ucd-list.txt";

        byte[] ivoaMd5;
        try (InputStream ivoaStream = new URL(ivoaResource).openStream()) {
            Assert.assertNotNull(ivoaStream);
            ivoaMd5 = computeMd5(ivoaStream);
        }

        byte[] localMd5;
        try (InputStream localStream = UCDValidatorTest.class.getClassLoader().getResourceAsStream(localResource)) {
            Assert.assertNotNull(localStream);
            localMd5 = computeMd5(localStream);
        }

        Assert.assertArrayEquals("MD5 of local and remote ucd-list.txt do not match", ivoaMd5, localMd5);
    }

    // TODO: This test has to be Commented unless testing
    //@Test
    public void testDeprecatedUCDFile() throws IOException, NoSuchAlgorithmException {
        String ivoaResource = "https://www.ivoa.net/Documents/UCD1+/20241218/ucd-list-deprecated.txt";
        String localResource = "ucd-list-deprecated.txt";

        byte[] ivoaMd5;
        try (InputStream ivoaStream = new URL(ivoaResource).openStream()) {
            Assert.assertNotNull(ivoaStream);
            ivoaMd5 = computeMd5(ivoaStream);
        }

        byte[] localMd5;
        try (InputStream localStream = UCDValidatorTest.class.getClassLoader().getResourceAsStream(localResource)) {
            Assert.assertNotNull(localStream);
            localMd5 = computeMd5(localStream);
        }

        Assert.assertArrayEquals("MD5 of local and remote ucd-list-deprecated.txt do not match", ivoaMd5, localMd5);
    }

    private byte[] computeMd5(InputStream inputStream) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        try (DigestInputStream dis = new DigestInputStream(inputStream, digest)) {
            byte[] buffer = new byte[8192];
            while (dis.read(buffer) != -1) {
                // just consuming the stream so the digest is updated
            }
        }
        return digest.digest();
    }

}
