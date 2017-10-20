package info.scandi.fusion.database.postgresql;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.dbunit.database.IDatabaseConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import info.scandi.fusion.conf.ExclusionSchemas;
import info.scandi.fusion.core.ConfigurationManager;

@RunWith(MockitoJUnitRunner.class)
public class AbstractPostgresqlWorkerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private ResultSet resultSet;

	@Before
	public void setUp() throws Exception {

		ConfigurationManager conf = mock(ConfigurationManager.class);
		ExclusionSchemas exclusionSchema = mock(ExclusionSchemas.class);
		when(exclusionSchema.getExclusionSchema()).thenReturn(Arrays.asList("admin"));
		when(conf.getDatabase().getLiquibase().getExclusionSchemas()).thenReturn(exclusionSchema);

		IDatabaseConnection databaseConnect = mock(IDatabaseConnection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		resultSet = mock(ResultSet.class);
		when(databaseConnect.getConnection().createStatement()).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		doNothing().when(statement).executeQuery();

		PostgresqlWorker pw = mock(PostgresqlWorker.class);
		doCallRealMethod().when(pw).cleanSequence();

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		String[] exclusionTables = new String[] { "" };
		try {
			when(resultSet.next()).thenReturn(true, false);
			when(resultSet.getString("schemaname")).thenReturn("public");
			when(resultSet.getString("tableName")).thenReturn("user");
			when(resultSet.getString("sequencename")).thenReturn("user_seq");
			when(resultSet.getString("primaryKey")).thenReturn("id");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fail("Not yet implemented");
	}

}
