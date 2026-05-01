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

package ca.nrc.cadc.tap.schema.validator.ucd;

import ca.nrc.cadc.tap.schema.validator.ValidationResult;
import ca.nrc.cadc.tap.schema.validator.ValidatorConfig;
import ca.nrc.cadc.tap.schema.validator.Violation;
import ca.nrc.cadc.tap.schema.validator.ViolationType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;

/**
 * Validates UCD1+ strings against the IVOA rules as defined in "UCD - An IVOA Standard for Unified Content Descriptors" v1.1
 * And "UCD1+ controlled vocabulary" v1.6
 */
public final class UCDValidator {

    private static final Logger log = Logger.getLogger(UCDValidator.class);

    private final ValidatorConfig config;

    public UCDValidator() {
        this(ValidatorConfig.lax());
    }

    public UCDValidator(ValidatorConfig config) {
        this.config = config;
    }

    /**
     * Validates a UCD1+ string and returns a detailed result.
     *
     * @param ucd the UCD string to validate
     * @return a {@link ValidationResult} describing validity and any violations
     */
    public ValidationResult validate(String ucd) {
        log.debug("Validating UCD string: " + ucd);
        List<Violation> violations = new ArrayList<>();

        if (ucd != null) {
            ucd = ucd.trim();
        }

        if (ucd == null || ucd.isEmpty()) {
            return new ValidationResult(null, violations, config);
        }

        if (ucd.startsWith(";") || ucd.endsWith(";")) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "UCD must not start or end with a semicolon"));
        }

        // split into individual words
        String[] words = ucd.split(";", -1);

        for (int i = 0; i < words.length; i++) {
            String word = words[i];

            Optional<UCDWord> vocabEntry = UCDVocabulary.lookup(word);
            if (vocabEntry.isEmpty()) {
                violations.add(new Violation(ViolationType.UCD_UNKNOWN_WORD,
                        "Word '" + word + "' at position " + (i + 1)
                                + " is not in the IVOA UCD1+ controlled vocabulary"));
                continue;
            }
            if (vocabEntry.get().isDeprecated()) {
                violations.add(new Violation(ViolationType.UCD_DEPRECATED_WORD,
                        "Word '" + word + "' at position " + (i + 1)
                                + " is deprecated. Suggested replacement: " + vocabEntry.get().getReplacement() + "."));
                continue;
            }

            UCDWord ucdWord = vocabEntry.get();
            boolean isPrimary = (i == 0);

            // primary word must not be S-only
            if (isPrimary && !ucdWord.canBePrimary()) {
                violations.add(new Violation(ViolationType.UCD_POSITION_MISMATCH,
                        "Word '" + word + "' has syntax flag "
                                + ucdWord.getFlag().name()
                                + " (secondary-only) and cannot be used as the primary word"));
            }

            // secondary word must not be P-only
            if (!isPrimary && !ucdWord.canBeSecondary()) {
                violations.add(new Violation(ViolationType.UCD_POSITION_MISMATCH,
                        "Word '" + word + "' at position " + (i + 1)
                                + " has syntax flag " + ucdWord.getFlag().name()
                                + " (primary-only) and cannot be used as a secondary word"));
            }
        }
        return new ValidationResult(ucd, violations, config);
    }

}

