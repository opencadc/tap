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

import java.util.Optional;

public enum Prefix {

    DA("da", "deca", PrefixType.SI),
    H("h", "hecto", PrefixType.SI),
    K("k", "kilo", PrefixType.SI),
    M("M", "mega", PrefixType.SI),
    G("G", "giga", PrefixType.SI),
    T("T", "tera", PrefixType.SI),
    P("P", "peta", PrefixType.SI),
    E("E", "exa", PrefixType.SI),
    Z("Z", "zetta", PrefixType.SI),
    Y("Y", "yotta", PrefixType.SI),
    R("R", "ronna", PrefixType.SI),
    Q("Q", "quetta", PrefixType.SI),

    D("d", "deci", PrefixType.SI),
    C("c", "centi", PrefixType.SI),
    MLI("m", "milli", PrefixType.SI),
    U("u", "micro", PrefixType.SI),   // ASCII only
    N("n", "nano", PrefixType.SI),
    PICO("p", "pico", PrefixType.SI),
    F("f", "femto", PrefixType.SI),
    A("a", "atto", PrefixType.SI),
    ZEPTO("z", "zepto", PrefixType.SI),
    YOTTO("y", "yocto", PrefixType.SI),
    RONTO("r", "ronto", PrefixType.SI),
    QO("q", "quecto", PrefixType.SI),

    // ---- Binary Prefixes ----
    KI("Ki", "kibi", PrefixType.BI),
    MI("Mi", "mebi", PrefixType.BI),
    GI("Gi", "gibi", PrefixType.BI),
    TI("Ti", "tebi", PrefixType.BI),
    PI("Pi", "pebi", PrefixType.BI),
    EI("Ei", "exbi", PrefixType.BI),
    ZI("Zi", "zebi", PrefixType.BI),
    YI("Yi", "yobi", PrefixType.BI);

    private final String symbol;
    private final String name;
    private final PrefixType type;

    Prefix(String symbol, String name, PrefixType type) {
        this.symbol = symbol;
        this.name = name;
        this.type = type;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getName() {
        return name;
    }

    public PrefixType getType() {
        return type;
    }

    public static Optional<Prefix> fromSymbol(String symbol) {
        for (Prefix p : values()) {
            if (p.symbol.equals(symbol)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }
}
