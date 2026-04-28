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

package ca.nrc.cadc.tap.schema.validator.adql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Enum representing reserved keywords for SQL and ADQL.
 */
public enum ReservedKeyword {

    // SQL reserved keywords
    ABSOLUTE("ABSOLUTE"), ACTION("ACTION"), ADD("ADD"), ALL("ALL"), ALLOCATE("ALLOCATE"), ALTER("ALTER"), AND("AND"), ANY("ANY"),
    ARE("ARE"), AS("AS"), ASC("ASC"), ASSERTION("ASSERTION"), AT("AT"), AUTHORIZATION("AUTHORIZATION"), AVG("AVG"), BEGIN("BEGIN"),
    BETWEEN("BETWEEN"), BIT("BIT"), BIT_LENGTH("BIT_LENGTH"), BOTH("BOTH"), BY("BY"), CASCADE("CASCADE"), CASCADED("CASCADED"),
    CASE("CASE"), CAST("CAST"), CATALOG("CATALOG"), CHAR("CHAR"), CHARACTER("CHARACTER"), CHAR_LENGTH("CHAR_LENGTH"),
    CHARACTER_LENGTH("CHARACTER_LENGTH"), CHECK("CHECK"), CLOSE("CLOSE"), COALESCE("COALESCE"), COLLATE("COLLATE"), COLLATION("COLLATION"),
    COLUMN("COLUMN"), COMMIT("COMMIT"), CONNECT("CONNECT"), CONNECTION("CONNECTION"), CONSTRAINT("CONSTRAINT"), CONSTRAINTS("CONSTRAINTS"),
    CONTINUE("CONTINUE"), CONVERT("CONVERT"), CORRESPONDING("CORRESPONDING"), COUNT("COUNT"), CREATE("CREATE"), CROSS("CROSS"),
    CURRENT("CURRENT"), CURRENT_DATE("CURRENT_DATE"), CURRENT_TIME("CURRENT_TIME"), CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"),
    CURRENT_USER("CURRENT_USER"), CURSOR("CURSOR"), DATE("DATE"), DAY("DAY"), DEALLOCATE("DEALLOCATE"), DECIMAL("DECIMAL"),
    DECLARE("DECLARE"), DEFAULT("DEFAULT"), DEFERRABLE("DEFERRABLE"), DEFERRED("DEFERRED"), DELETE("DELETE"), DESC("DESC"),
    DESCRIBE("DESCRIBE"), DESCRIPTOR("DESCRIPTOR"), DIAGNOSTICS("DIAGNOSTICS"), DISCONNECT("DISCONNECT"), DISTINCT("DISTINCT"),
    DOMAIN("DOMAIN"), DOUBLE("DOUBLE"), DROP("DROP"), ELSE("ELSE"), END("END"), ESCAPE("ESCAPE"), EXCEPT("EXCEPT"), EXCEPTION("EXCEPTION"),
    EXEC("EXEC"), EXECUTE("EXECUTE"), EXISTS("EXISTS"), EXTERNAL("EXTERNAL"), EXTRACT("EXTRACT"), FALSE("FALSE"), FETCH("FETCH"),
    FIRST("FIRST"), FLOAT("FLOAT"), FOR("FOR"), FOREIGN("FOREIGN"), FOUND("FOUND"), FROM("FROM"), FULL("FULL"), GET("GET"), GLOBAL("GLOBAL"),
    GO("GO"), GOTO("GOTO"), GRANT("GRANT"), GROUP("GROUP"), HAVING("HAVING"), HOUR("HOUR"), IDENTITY("IDENTITY"), IMMEDIATE("IMMEDIATE"),
    IN("IN"), INDICATOR("INDICATOR"), INITIALLY("INITIALLY"), INNER("INNER"), INPUT("INPUT"), INSENSITIVE("INSENSITIVE"), INSERT("INSERT"),
    INT("INT"), INTEGER("INTEGER"), INTERSECT("INTERSECT"), INTERVAL("INTERVAL"), INTO("INTO"), IS("IS"), ISOLATION("ISOLATION"), JOIN("JOIN"),
    KEY("KEY"), LANGUAGE("LANGUAGE"), LAST("LAST"), LEADING("LEADING"), LEFT("LEFT"), LEVEL("LEVEL"), LIKE("LIKE"), LOCAL("LOCAL"), LOWER("LOWER"),
    MATCH("MATCH"), MAX("MAX"), MIN("MIN"), MINUTE("MINUTE"), MODULE("MODULE"), MONTH("MONTH"), NAMES("NAMES"), NATIONAL("NATIONAL"), NATURAL("NATURAL"),
    NCHAR("NCHAR"), NEXT("NEXT"), NO("NO"), NOT("NOT"), NULL("NULL"), NULLIF("NULLIF"), NUMERIC("NUMERIC"), OCTET_LENGTH("OCTET_LENGTH"), OF("OF"), ON("ON"),
    ONLY("ONLY"), OPEN("OPEN"), OPTION("OPTION"), OR("OR"), ORDER("ORDER"), OUTER("OUTER"), OUTPUT("OUTPUT"), OVERLAPS("OVERLAPS"), PAD("PAD"),
    PARTIAL("PARTIAL"), POSITION("POSITION"), PRECISION("PRECISION"), PREPARE("PREPARE"), PRESERVE("PRESERVE"), PRIMARY("PRIMARY"), PRIOR("PRIOR"),
    PRIVILEGES("PRIVILEGES"), PROCEDURE("PROCEDURE"), PUBLIC("PUBLIC"), READ("READ"), REAL("REAL"), REFERENCES("REFERENCES"), RELATIVE("RELATIVE"),
    RESTRICT("RESTRICT"), REVOKE("REVOKE"), RIGHT("RIGHT"), ROLLBACK("ROLLBACK"), ROWS("ROWS"), SCHEMA("SCHEMA"), SCROLL("SCROLL"), SECOND("SECOND"),
    SECTION("SECTION"), SELECT("SELECT"), SESSION("SESSION"), SESSION_USER("SESSION_USER"), SET("SET"), SIZE("SIZE"), SMALLINT("SMALLINT"), SOME("SOME"),
    SPACE("SPACE"), SQL("SQL"), SQLCODE("SQLCODE"), SQLERROR("SQLERROR"), SQLSTATE("SQLSTATE"), SUBSTRING("SUBSTRING"), SUM("SUM"),
    SYSTEM_USER("SYSTEM_USER"), TABLE("TABLE"), TEMPORARY("TEMPORARY"), THEN("THEN"), TIME("TIME"), TIMESTAMP("TIMESTAMP"),
    TIMEZONE_HOUR("TIMEZONE_HOUR"), TIMEZONE_MINUTE("TIMEZONE_MINUTE"), TO("TO"), TRAILING("TRAILING"), TRANSACTION("TRANSACTION"),
    TRANSLATE("TRANSLATE"), TRANSLATION("TRANSLATION"), TRIM("TRIM"), TRUE("TRUE"), UNION("UNION"), UNIQUE("UNIQUE"), UNKNOWN("UNKNOWN"),
    UPDATE("UPDATE"), UPPER("UPPER"), USAGE("USAGE"), USER("USER"), USING("USING"), VALUE("VALUE"), VALUES("VALUES"), VARCHAR("VARCHAR"),
    VARYING("VARYING"), VIEW("VIEW"), WHEN("WHEN"), WHENEVER("WHENEVER"), WHERE("WHERE"), WITH("WITH"), WORK("WORK"), WRITE("WRITE"),
    YEAR("YEAR"), ZONE("ZONE"),
    END_EXEC("END-EXEC"),

    // ADQL reserved keywords
    // Mathematical functions and operators
    ABS("ABS"), ACOS("ACOS"), ASIN("ASIN"), ATAN("ATAN"), ATAN2("ATAN2"), CEILING("CEILING"), COS("COS"), COT("COT"), DEGREES("DEGREES"),
    EXP("EXP"), FLOOR("FLOOR"), LOG("LOG"), LOG10("LOG10"), MOD("MOD"), PI("PI"), POWER("POWER"), RADIANS("RADIANS"), RAND("RAND"),
    ROUND("ROUND"), SIN("SIN"), SQRT("SQRT"), TAN("TAN"), TRUNCATE("TRUNCATE"),

    // Geometric functions and operators
    AREA("AREA"), BOX("BOX"), CENTROID("CENTROID"), CIRCLE("CIRCLE"), CONTAINS("CONTAINS"), COORD1("COORD1"), COORD2("COORD2"),
    COORDSYS("COORDSYS"), DISTANCE("DISTANCE"), INTERSECTS("INTERSECTS"), POINT("POINT"), POLYGON("POLYGON"), REGION("REGION"),

    // CAST function and datatypes
    BIGINT("BIGINT"),

    // String functions and operators
    ILIKE("ILIKE"),

    // Conversion functions
    IN_UNIT("IN_UNIT"),

    // Cardinality
    OFFSET("OFFSET"), TOP("TOP");

    private final String word;

    ReservedKeyword(String word) {
        this.word = word;
    }

    private static final Map<String, ReservedKeyword> RESERVED_KEYWORDS;

    static {
        Map<String, ReservedKeyword> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ReservedKeyword keyword : values()) {
            if (map.containsKey(keyword.word)) {
                throw new IllegalStateException("BUG: Duplicate Reserved Keyword : " + keyword.word);
            }
            map.put(keyword.word, keyword);
        }
        RESERVED_KEYWORDS = Collections.unmodifiableMap(map);
    }

    public static Set<String> getAllReservedWords() {
        return RESERVED_KEYWORDS.keySet();
    }

    public static boolean isReserved(String word) {
        return RESERVED_KEYWORDS.containsKey(word);
    }
}
