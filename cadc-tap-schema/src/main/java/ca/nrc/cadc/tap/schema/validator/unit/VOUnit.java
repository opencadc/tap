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

package ca.nrc.cadc.tap.schema.validator.unit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Enum representing VOUnits as defined by the IVOA VOUnits specification.
 * Each unit includes its symbol, description, and allowed prefix types.
 * Used for validating and mapping VOUnit strings in TAP schema.
 *
 * @see <a href="https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html">IVOA VOUnits Specification</a>
 */
public enum VOUnit {

    PERCENT("%", "percent", Collections.emptyList()),
    AMPERE("A", "ampere", List.of(PrefixType.SI)),
    JULIAN_YEAR("a", "julian year", List.of(PrefixType.SI)),
    ADU("adu", "ADU", List.of(PrefixType.SI)),
    ANGSTROM("Angstrom", "angstrom", Collections.emptyList()), // deprecated
    ANGSTROM_LOWER("angstrom", "angstrom", Collections.emptyList()), // deprecated
    ARCMIN("arcmin", "arc minute", List.of(PrefixType.SI)),
    ARCSEC("arcsec", "arc second", List.of(PrefixType.SI)),
    AU("AU", "astronomical unit", Collections.emptyList()),
    AU_LOWER("au", "astronomical unit", Collections.emptyList()),
    BESSELIAN_YEAR("Ba", "besselian year", Collections.emptyList()), // deprecated
    BARN("barn", "barn", List.of(PrefixType.SI)), // deprecated
    BEAM("beam", "beam", List.of(PrefixType.SI)),
    BIN("bin", "bin", List.of(PrefixType.SI)),
    BIT("bit", "bit", List.of(PrefixType.SI, PrefixType.BI)),
    BYTE("byte", "byte", List.of(PrefixType.SI, PrefixType.BI)),
    BYTE_UPPER("B", "byte", List.of(PrefixType.SI, PrefixType.BI)),
    COULOMB("C", "coulomb", List.of(PrefixType.SI)),
    CANDELA("cd", "candela", List.of(PrefixType.SI)),
    CHANNEL("chan", "channel", List.of(PrefixType.SI)),
    COUNT("count", "number", List.of(PrefixType.SI)),
    //CRAB("Crab", "crab", Collections.emptyList()), // Not supported in VOUnit String
    CT("ct", "number", List.of(PrefixType.SI)),
    //JULIAN_CENTURY("cy", "julian century", Collections.emptyList()), // Not supported in VOUnit String
    DAY("d", "day", List.of(PrefixType.SI)),
    DECIBEL("dB", "decibel", Collections.emptyList()),
    DEBYE("D", "debye", List.of(PrefixType.SI)),
    DEGREE("deg", "degree (angle)", List.of(PrefixType.SI)),
    ERG("erg", "erg", List.of(PrefixType.SI)), // deprecated
    ELECTRON_VOLT("eV", "electron volt", List.of(PrefixType.SI)),
    FARAD("F", "farad", List.of(PrefixType.SI)),
    GRAM("g", "gramme", List.of(PrefixType.SI)),
    GAUSS("G", "gauss", List.of(PrefixType.SI)), // deprecated
    HENRY("H", "henry", List.of(PrefixType.SI)),
    HOUR("h", "hour", List.of(PrefixType.SI)),
    HERTZ("Hz", "hertz", List.of(PrefixType.SI)),
    JOULE("J", "joule", List.of(PrefixType.SI)),
    JY("Jy", "jansky", List.of(PrefixType.SI)),
    KELVIN("K", "kelvin", List.of(PrefixType.SI)),
    LUMEN("lm", "lumen", List.of(PrefixType.SI)),
    LUX("lx", "lux", List.of(PrefixType.SI)),
    LIGHT_YEAR("lyr", "light year", List.of(PrefixType.SI)),
    METER("m", "meter", List.of(PrefixType.SI)),
    MAG("mag", "magnitudes", List.of(PrefixType.SI)),
    MAS("mas", "milliarcsecond", Collections.emptyList()),
    MINUTE("min", "minute (time)", List.of(PrefixType.SI)),
    MOLE("mol", "mole", List.of(PrefixType.SI)),
    NEWTON("N", "newton", List.of(PrefixType.SI)),
    OHM("Ohm", "ohm", List.of(PrefixType.SI)),
    //OHM_LOWER("ohm", "ohm", Collections.emptyList()), // Not supported in VOUnit String
    PASCAL("Pa", "pascal", List.of(PrefixType.SI)),
    PARSEC("pc", "parsec", List.of(PrefixType.SI)),
    PHOTON("ph", "photon", List.of(PrefixType.SI)),
    PHOTON_FULL("photon", "photon", List.of(PrefixType.SI)),
    PIX("pix", "pixel", List.of(PrefixType.SI)),
    PIXEL("pixel", "pixel", List.of(PrefixType.SI)),
    RAYLEIGH("R", "rayleigh", List.of(PrefixType.SI)),
    RADIAN("rad", "radian", List.of(PrefixType.SI)),
    RYDBERG("Ry", "rydberg", List.of(PrefixType.SI)),
    SECOND("s", "second (time)", List.of(PrefixType.SI)),
    SIEMENS("S", "siemens", List.of(PrefixType.SI)),
    SOLAR_LUMINOSITY("solLum", "luminosity", List.of(PrefixType.SI)),
    SOLAR_MASS("solMass", "solar mass", List.of(PrefixType.SI)),
    SOLAR_RADIUS("solRad", "solar radius", Collections.emptyList()),
    STERADIAN("sr", "steradian", List.of(PrefixType.SI)),
    TESLA("T", "tesla", List.of(PrefixType.SI)),
    TROPICAL_YEAR("ta", "year tropical", Collections.emptyList()), // deprecated
    AMU("u", "AMU", List.of(PrefixType.SI)),
    VOLT("V", "volt", List.of(PrefixType.SI)),
    VOXEL("voxel", "voxel", List.of(PrefixType.SI)),
    WATT("W", "watt", List.of(PrefixType.SI)),
    WEBER("Wb", "weber", List.of(PrefixType.SI)),
    JULIAN_YEAR_YR("yr", "julian year", List.of(PrefixType.SI)),

    // Miscellaneous VOUnits From Table 5, which are not in Table 1 & 2
    SUN("Sun", "sun", Collections.emptyList()),

    ;

    private final String symbol;
    private final String description;
    private final List<PrefixType> allowedPrefixes;

    VOUnit(String symbol, String description,
           List<PrefixType> allowedPrefixes) {
        this.symbol = symbol;
        this.description = description;
        this.allowedPrefixes = allowedPrefixes;
    }

    private static final Map<String, VOUnit> SYMBOL_MAP;

    static {
        Map<String, VOUnit> map = new HashMap<>();
        for (VOUnit unit : values()) {
            if (map.containsKey(unit.symbol)) {
                throw new IllegalStateException("Duplicate VOUnit symbol: " + unit.symbol);
            }
            map.put(unit.symbol, unit);
        }
        SYMBOL_MAP = Collections.unmodifiableMap(map);
    }

    public static Set<String> getAllSymbols() {
        return SYMBOL_MAP.keySet();
    }

    public static Optional<VOUnit> fromSymbol(String symbol) {
        return Optional.ofNullable(SYMBOL_MAP.get(symbol));
    }

    public boolean isAllowedPrefix(PrefixType type) {
        return allowedPrefixes.contains(type);
    }
}

