package kz.kbtu.pastebin;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Runs the legacy matrix as independent JUnit DynamicTests, one scenario per result. */
class LegacyScenariosJUnitTest {
    @TestFactory
    Stream<DynamicTest> allProtocolScenarios() {
        return IntStream.range(0, 33).mapToObj(i -> DynamicTest.dynamicTest("legacy scenario " + (i + 1), () ->
                assertEquals(0, TestMain.runScenario(i))));
    }
}
