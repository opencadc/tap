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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Decides which {@link ViolationType}s are treated as errors (failing).
 * Kinds not in the error set are downgraded to warnings.
 */
public class ValidatorConfig {

    public enum Severity {
        ERROR, WARNING
    }

    public enum ConfigType {
        STRICT, LAX, NONE
    }

    private final ConfigType configType;

    /** Violation types that are structurally mandatory — can never be warnings. */
    private static final Set<ViolationType> ALWAYS_ERRORS = Collections.unmodifiableSet(EnumSet.of(
            ViolationType.NULL_OR_BLANK,
            ViolationType.STRUCTURAL,
            ViolationType.IDENTIFIER_INVALID_CHAR,
            ViolationType.IDENTIFIER_RESERVED_KEYWORD // allowing quoted identifiers if config is not "strict"
    ));

    private final Set<ViolationType> errorTypes;

    private ValidatorConfig(Set<ViolationType> errorTypes, ConfigType configType) {
        EnumSet<ViolationType> merged = EnumSet.copyOf(ALWAYS_ERRORS);
        merged.addAll(errorTypes);
        this.errorTypes = Collections.unmodifiableSet(merged);
        this.configType = configType;
    }

    // factories

    /** Every Violation is an error — full IVOA compliance required. */
    public static ValidatorConfig strict() {
        return new ValidatorConfig(EnumSet.allOf(ViolationType.class), ConfigType.STRICT);
    }

    /** Default configuration.
     *  A set including ALWAYS_ERRORS and below listed are considered errors.*/
    public static ValidatorConfig lax() {
        return new ValidatorConfig(EnumSet.of(
                ViolationType.UCD_UNKNOWN_WORD,
                ViolationType.UCD_POSITION_MISMATCH,
                ViolationType.VOUNIT_CASE_MISMATCH
        ), ConfigType.LAX);
    }

    /** Only ALWAYS_ERRORS set is considered errors. */
    public static ValidatorConfig none() {
        return new ValidatorConfig(Collections.emptySet(), ConfigType.NONE);
    }

    public ConfigType getConfigType() {
        return configType;
    }

    public boolean isError(ViolationType violationType) {
        return errorTypes.contains(violationType);
    }

    public Severity severityFor(ViolationType violationType) {
        return isError(violationType) ? Severity.ERROR : Severity.WARNING;
    }
}
