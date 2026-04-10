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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Holds the IVOA UCD1+ controlled vocabulary.
 *
 * <p>The latest official word list (v1.6, Dec 2024) is available at:
 * <a href="https://www.ivoa.net/documents/UCD1+/20241218/">UCD1+</a>
 */
public abstract class UCDVocabulary {

    private static final Logger log = Logger.getLogger(UCDVocabulary.class);

    public static final String RESOURCE_UCD_LIST = "ucd-list.txt";
    public static final String RESOURCE_DEPRECATED_UCD_LIST = "ucd-list-deprecated.txt";

    private static Map<String, UCDWord> words;

    static {
        int validWords = 0;
        int deprecatedWords = 0;
        try (InputStream ucdListStream = UCDVocabulary.class.getClassLoader().getResourceAsStream(RESOURCE_UCD_LIST)) {
            if (ucdListStream == null) {
                throw new IOException("UCD word list not found at the classpath : '" + RESOURCE_UCD_LIST + "'");
            }
            loadUCDWords(ucdListStream);
            validWords = words.size();

            try (InputStream deprecatedUcdListStream = UCDVocabulary.class.getClassLoader().getResourceAsStream(RESOURCE_DEPRECATED_UCD_LIST)) {
                if (deprecatedUcdListStream != null) {
                    loadDeprecatedUCDWords(deprecatedUcdListStream);
                }
            }
            deprecatedWords = words.size() - validWords;
        } catch (Exception e) {
            log.error("BUG: Failed to load UCD word list from resource '" + RESOURCE_UCD_LIST + "': " + e.getMessage());
        }
        log.info("Loaded " + validWords + " valid UCD words and " + deprecatedWords + " deprecated UCD words.");
    }

    private static void loadUCDWords(InputStream inputStream) throws IOException {
        log.info("Loading UCD word list from the default resource: " + RESOURCE_UCD_LIST);
        words = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\|", 3);
                if (parts.length < 2) {
                    continue; // malformed line – skip
                }
                String flagStr = parts[0].strip().toUpperCase();
                String word = parts[1].strip();
                String desc = parts.length == 3 ? parts[2].strip() : "";

                UCDWord.SyntaxFlag flag;
                try {
                    flag = UCDWord.SyntaxFlag.valueOf(flagStr);
                } catch (IllegalArgumentException e) {
                    throw new IOException("Invalid UCD word flag: " + flagStr + " in line: " + line, e);
                }
                // store with original capitalisation; look up case-insensitively
                words.put(word, new UCDWord(word, flag, desc));
            }
        }
    }

    private static void loadDeprecatedUCDWords(InputStream inputStream) throws IOException {
        log.info("Loading Deprecated UCD word list from the default resource: " + RESOURCE_DEPRECATED_UCD_LIST);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split(" ", 2);
                if (parts.length != 2) {
                    continue; // malformed line – skip
                }
                String deprecatedWord = parts[0].strip();
                String replacementWord = parts[1].strip();

                words.put(deprecatedWord, new UCDWord(deprecatedWord, replacementWord));
            }
        }
    }

    /**
     * Looks up a word from the vocabulary (case-insensitive).
     *
     * @param word the atom to look up
     * @return the UCDWord if present, empty otherwise
     */
    public static Optional<UCDWord> lookup(String word) {
        if (word == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(words.get(word));
    }

    /**
     * Returns true if the word exists in the controlled vocabulary.
     */
    public static boolean contains(String word) {
        return lookup(word).isPresent();
    }

    /**
     * Returns an unmodifiable view of all registered words.
     */
    public static Set<String> getAllWords() {
        return words.keySet();
    }

}