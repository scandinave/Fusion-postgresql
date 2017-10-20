/**
 * 
 */
package info.scandi.fusion.database;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

import org.dbunit.database.IDatabaseConnection;

import info.scandi.fusion.database.bdd.TableBDD;
import info.scandi.fusion.exception.FusionException;
import info.scandi.fusion.exception.RequestException;

/**
 * Cleans all data from a database. This process don't alter the database
 * structure.
 * 
 * @author Scandinave
 */
@Named
@ApplicationScoped
public class Cleaner implements Serializable {

	/**
	 * serialVersionUID long.
	 */
	private static final long serialVersionUID = 142577290372523301L;

	@Inject
	private Logger LOGGER;
	@Inject
	private IDatabaseConnection databaseConnect;
	@Inject
	DatabaseProducer databaseProducer;

	private static final String COMMAND_SQL = "TRUNCATE ";
	private static final String SEPARATEUR_SQL = ", ";
	private static final String END_COMMAND_SQL = " RESTART IDENTITY CASCADE";

	/**
	 * Starts the clean process.
	 * 
	 * @param tables
	 * 
	 * @throws FusionException
	 */
	public void start(Set<TableBDD> tables) throws FusionException {
		emptyBase(tables);
	}

	private void emptyBase(Set<TableBDD> tables) throws FusionException {
		LOGGER.fine("purging the database");
		StringBuilder sql = new StringBuilder();
		sql.append(COMMAND_SQL);
		tables.forEach(table -> {
			sql.append(table);
			sql.append(SEPARATEUR_SQL);
		});
		String lastTwoChar = sql.substring(sql.length() - 2);
		if (SEPARATEUR_SQL.equals(lastTwoChar)) {
			sql.delete(sql.length() - 2, sql.length());
		}
		sql.append(END_COMMAND_SQL);
		try (Statement statement = databaseConnect.getConnection().createStatement()) {
			statement.executeUpdate(sql.toString());
		} catch (SQLException e) {
			throw new FusionException(new RequestException(e));
		}
	}
}
