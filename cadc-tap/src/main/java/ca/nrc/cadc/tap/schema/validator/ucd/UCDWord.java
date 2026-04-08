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

/**
 * Represents a single UCD1+ atom (word) from the IVOA controlled vocabulary.
 * Each word carries a syntax flag that governs where it may appear in a UCD string.
 * Syntax flags (as defined by IVOA REC-UCDlist):
 * P - Primary only:   may ONLY appear as the first (primary) word in a UCD
 * S - Secondary only: may ONLY appear as a secondary (qualifying) word
 * Q - Both:           may appear in either primary or secondary position
 * V - Vector:         like Q, but describes a vector quantity; may be followed
 * by a secondary describing the axis/reference frame
 * E - Deprecated flag used in early versions; treated as Q for compatibility
 */
public class UCDWord {

    /**
     * Allowed syntax flag characters.
     */
    public enum SyntaxFlag {
        P, // primary only
        S, // secondary only
        Q, // primary or secondary
        E, // photometric quantity – treated as Q
        V, // vector – treated as Q
        C  // color index – treated as Q
    }

    private final String word;
    private final SyntaxFlag flag;
    private final String description;
    private boolean deprecated = false;
    private String replacement = null;

    /**
     * Constructor for Valid UCD words.
     *
     * @param word        UCD atom
     * @param flag        syntax flag
     * @param description description of the word
     */
    public UCDWord(String word, SyntaxFlag flag, String description) {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("UCD word must not be blank");
        }
        this.word = word;
        this.flag = flag;
        this.description = description != null ? description : "";
    }

    /**
     * Constructor for deprecated UCD words.
     * <p>Note: SyntaxFlag is set to Q by default for deprecated word.</p>
     *
     * @param word        the deprecated UCD atom
     * @param replacement the recommended replacement word, or {@code null} if none
     */
    public UCDWord(String word, String replacement) {
        this(word, SyntaxFlag.Q, "Deprecated word");
        this.deprecated = true;
        this.replacement = replacement;
    }


    public String getWord() {
        return word;
    }

    public SyntaxFlag getFlag() {
        return flag;
    }

    public String getDescription() {
        return description;
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    public String getReplacement() {
        return replacement;
    }

    public boolean canBePrimary() {
        return flag == SyntaxFlag.P || flag == SyntaxFlag.Q || flag == SyntaxFlag.C
                || flag == SyntaxFlag.V || flag == SyntaxFlag.E;
    }

    public boolean canBeSecondary() {
        return flag == SyntaxFlag.S || flag == SyntaxFlag.Q || flag == SyntaxFlag.C
                || flag == SyntaxFlag.V || flag == SyntaxFlag.E;
    }

    @Override
    public String toString() {
        return flag.name() + " | " + word + " | " + description;
    }
}