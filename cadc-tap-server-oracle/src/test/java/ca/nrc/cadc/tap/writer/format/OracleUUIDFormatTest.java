package ca.nrc.cadc.tap.writer.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.UUID;
import org.junit.Test;

public class OracleUUIDFormatTest {

    @Test
    public void parseUuidString() {
        OracleUUIDFormat subject = new OracleUUIDFormat();
        UUID expected = UUID.fromString("00000000-0000-0000-68b8-013d15b71dda");

        assertEquals(expected, subject.parse("00000000-0000-0000-68b8-013d15b71dda"));
        assertNull(subject.parse(null));
    }

    @Test
    public void formatUuid() {
        OracleUUIDFormat subject = new OracleUUIDFormat();
        UUID uuid = UUID.fromString("00000000-0000-0000-68b8-013d15b71dda");

        assertEquals("00000000-0000-0000-68b8-013d15b71dda", subject.format(uuid));
    }
}
