package kafkaapp;

import static org.junit.Assert.*;

import org.junit.Test;

public class KafkaAppTest {

	@Test
	public void testSandboxEquals() {
		assertEquals(0, KafkaApp.sandbox());
	}
	@Test
	public void testSandboxNotEquals() {
		assertNotEquals(-1, KafkaApp.sandbox());
	}
}
