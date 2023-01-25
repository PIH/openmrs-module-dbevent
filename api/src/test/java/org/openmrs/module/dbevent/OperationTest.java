package org.openmrs.module.dbevent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OperationTest extends BaseDbEventTest {

	@BeforeEach
	@Override
	public void setup() {
		super.setup();
	}

	@AfterEach
	@Override
	public void teardown() {
		super.teardown();
	}

	@Test
	public void shouldParseStringToOperation() {
		assertThat(Operation.parse("r"), equalTo(Operation.READ));
		assertThat(Operation.parse("c"), equalTo(Operation.INSERT));
		assertThat(Operation.parse("u"), equalTo(Operation.UPDATE));
		assertThat(Operation.parse("d"), equalTo(Operation.DELETE));
	}

	@Test
	public void shouldThrowExceptionIfUnknownValuePassedToParse() {
		assertThrows(IllegalArgumentException.class, () -> Operation.parse("q"));
	}
}
