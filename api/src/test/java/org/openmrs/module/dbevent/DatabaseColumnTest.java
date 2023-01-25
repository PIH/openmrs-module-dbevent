package org.openmrs.module.dbevent;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class DatabaseColumnTest {

    @Test
    public void shouldReturnProperties() {
        DatabaseColumn column = new DatabaseColumn("test", "location", "name", false);
        assertThat(column.getDatabaseName(), equalTo("test"));
        assertThat(column.getTableName(), equalTo("location"));
        assertThat(column.getColumnName(), equalTo("name"));
        assertFalse(column.isNullable());
        assertThat(column.getTableAndColumn(), equalTo("location.name"));
    }
}
