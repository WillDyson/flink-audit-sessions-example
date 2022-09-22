import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import com.cloudera.wdyson.flink.auditsession.Audit;

import org.junit.jupiter.api.Test;

class TestJson {
    private String readResource(String path) throws IOException, URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();

        return String.join("\n",
            Files.readAllLines(Paths.get(classLoader.getResource(path).toURI())));
    }

    @Test
    void simpleAuditParseTest() throws Exception {
        String json = readResource("audit-examples/kafka-1.json");

        Audit audit = Audit.fromJson(json);

        if (audit == null) {
            fail("Simple audit could not be parsed");
        }

        assertEquals(9, audit.repoType);
        assertEquals("wdyson", audit.reqUser);
    }
}
