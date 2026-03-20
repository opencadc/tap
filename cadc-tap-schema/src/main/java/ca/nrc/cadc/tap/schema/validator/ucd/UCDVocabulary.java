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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Holds the IVOA UCD1+ controlled vocabulary.
 *
 * <p>The latest official word list (v1.6, Dec 2024) is available at:
 * <a href="https://www.ivoa.net/documents/UCD1+/20241218/">UCD1+</a>
 */
public class UCDVocabulary {

    private static final Logger log = Logger.getLogger(UCDVocabulary.class);

    public static final String DEFAULT_RESOURCE = "https://www.ivoa.net/Documents/UCD1+/20241218/ucd-list.txt";

    private static Map<String, UCDWord> words;

    static {
        try (InputStream stream = new URL(DEFAULT_RESOURCE).openStream()) {
            if (stream == null) {
                log.info("UCD word list not found at '" + DEFAULT_RESOURCE + "'; " + "using the UCDWordEntry enum.");
                loadFromEnum();
            } else {
                parseStream(stream);
            }
        } catch (Exception e) {
            log.error("Failed to load UCD word list from resource '" + DEFAULT_RESOURCE + "': " + e.getMessage());
            loadFromEnum();
        }
        log.info("Loaded " + words.size() + " UCD word list.");
    }

    public UCDVocabulary() {
    }

    /**
     * Builds the vocabulary from the {@link UCDWordEntry} enum.
     */
    public static void loadFromEnum() {
        log.info("Loading UCD word list from UCDWordEntry enum.");
        words = new HashMap<>();
        for (UCDWordEntry entry : UCDWordEntry.values()) {
            words.put(entry.getWord().toLowerCase(), entry.toUcdWord());
        }
    }

    private static void parseStream(InputStream inputStream) throws IOException {
        log.info("Loading UCD word list from the default resource: " + DEFAULT_RESOURCE);
        words = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
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
                    continue; // unknown flag – skip entry
                }
                // store with original capitalisation; look up case-insensitively
                words.put(word.toLowerCase(), new UCDWord(word, flag, desc));
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
        return Optional.ofNullable(words.get(word.toLowerCase()));
    }

    /**
     * Returns true if the word exists in the controlled vocabulary.
     */
    public boolean contains(String word) {
        return lookup(word).isPresent();
    }

    /**
     * Returns an unmodifiable view of all registered words.
     */
    public Set<String> getAllWords() {
        return words.keySet();
    }

}