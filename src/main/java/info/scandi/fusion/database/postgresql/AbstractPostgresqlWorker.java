package info.scandi.fusion.database.postgresql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import javax.inject.Inject;

import info.scandi.fusion.conf.ExclusionSchemas;
import info.scandi.fusion.database.Cleaner;
import info.scandi.fusion.database.Saver;
import info.scandi.fusion.database.bdd.SequenceBDD;
import info.scandi.fusion.database.bdd.TableBDD;
import info.scandi.fusion.database.worker.AbstractWorker;
import info.scandi.fusion.exception.FusionException;
import info.scandi.fusion.exception.RequestException;

/**
 * Default implementation of the PostgreSQL DBMS worker.
 * 
 * @author Scandinave
 */
public abstract class AbstractPostgresqlWorker extends AbstractWorker {

	@Inject
	private Saver saver;

	@Inject
	private Cleaner cleaner;

	/**
	 * Default constructor.
	 * 
	 * @throws FusionException
	 */
	protected AbstractPostgresqlWorker() throws FusionException {
		super();
	}

	@Override
	public void start() throws FusionException {
		super.start();
		this.vacuum();
	}

	@Override
	public void clean(List<String> exclusionSchemas, List<String> exclusionTables) throws FusionException {
		cleaner.start(getTablesTypeTableWithExclusions(exclusionSchemas, exclusionTables));

	}

	@Override
	public void save() throws FusionException {
		if (this.conf.getDatabase().getBackup().isEnabled()) {
			LOGGER.info("Saving database...");
			saver.start(getAllTablesTypeTable());
		}

	}

	@Override
	public void restore() throws FusionException {
		super.restore();
		if (conf.getDatabase().getBackup().isEnabled()) {
			this.vacuum();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see dbunit.worker.IBDDWorker#toogleContrainte(boolean)
	 */
	@Override
	public void toogleContrainte(boolean toogle) throws FusionException {
		LOGGER.info(toogle ? "Enabling Constraints" : "Disabling Constraints");
		try {
			String update = "UPDATE pg_trigger SET tgenabled = " + (toogle ? "'O'" : "'D'");
			Statement statement = databaseConnect.getConnection().createStatement();
			statement.executeUpdate(update);
			statement.close();
		} catch (Exception e) {
			throw new FusionException(String.format("problem when %s constraints.", toogle ? "enabling" : "disabling"),
					e);
		}
	}

	/**
	 * Initializes sequence to 0.
	 * 
	 * @throws FusionException
	 */
	@Override
	public void cleanSequence() throws FusionException {
		LOGGER.fine("Updates sequences to 1");
		TreeSet<SequenceBDD> sequences = getSequences(conf.getDatabase().getLiquibase().getExclusionSchemas(), null);
		for (Iterator<SequenceBDD> iterator = sequences.iterator(); iterator.hasNext();) {
			SequenceBDD sequenceBDD = iterator.next();
			setSequence(sequenceBDD, "1");
		}
		LOGGER.fine("Sequences update finish.");
	}

	/**
	 * Synchronizes the sequence with data after table update.
	 * 
	 * @throws FusionException
	 */
	@Override
	public void majSequence() throws FusionException {
		TreeSet<SequenceBDD> sequences = getSequences(conf.getDatabase().getLiquibase().getExclusionSchemas(), null);
		for (SequenceBDD sequence : sequences) {
			setSequence(sequence, "(SELECT MAX(" + sequence.getTableBDD().getPrimaryKey() + ") FROM "
					+ sequence.getSchemaNamePointSequenceName() + ")");
		}
	}

	/**
	 * Return a SequenceBDD to handle bas on exclusionShcemas and
	 * exclusionTables.
	 * 
	 * @param resultSet
	 *            The resultSet containing a sequence to parse.
	 * @param exclusionSchemas
	 *            The list of schema to exclude for the processing
	 * @param exclusionTables
	 *            The list of table to exclude for processing
	 * @return The Sequence to handle if found.
	 * @throws SQLException
	 * @throws FusionException
	 *             TODO Optimise this method. Reduce complexity and take care to
	 *             break loop as needed
	 */
	private SequenceBDD parseSequences(ResultSet resultSet, ExclusionSchemas exclusionSchemas, String[] exclusionTables)
			throws SQLException, FusionException {
		String schemaName = resultSet.getString("schemaname");
		String tableName = resultSet.getString("tableName");
		String sequenceName = resultSet.getString("sequencename");
		String primaryKey = resultSet.getString("primaryKey");
		SequenceBDD result = null;
		if (exclusionSchemas != null) {
			for (String exclusionSchema : exclusionSchemas.getExclusionSchema()) {
				if (!schemaName.equals(exclusionSchema)) {
					if (exclusionTables != null) {
						for (int j = 0; j < exclusionTables.length; j++) {
							String exludeTable = exclusionTables[j];
							if (!tableName.equals(exludeTable)) {
								TableBDD tableBDD = new TableBDD(schemaName, tableName);
								tableBDD.setPrimaryKey(primaryKey);
								result = new SequenceBDD(tableBDD, sequenceName);
							}
						}
					} else {
						TableBDD tableBDD = new TableBDD(schemaName, tableName);
						tableBDD.setPrimaryKey(primaryKey);
						result = new SequenceBDD(tableBDD, sequenceName);
					}
				}
			}
		} else {
			TableBDD tableBDD = new TableBDD(schemaName, tableName);
			tableBDD.setPrimaryKey(primaryKey);
			result = new SequenceBDD(tableBDD, sequenceName);
		}
		return result;
	}

	/**
	 * Returns list of sequences in the database.
	 * 
	 * @return
	 * @throws FusionException
	 */
	// TODO rewrite the loop
	private TreeSet<SequenceBDD> getSequences(ExclusionSchemas exclusionSchemas, String[] exclusionTables)
			throws FusionException {
		TreeSet<SequenceBDD> sequences = new TreeSet<SequenceBDD>();
		try {
			String sql = "SELECT n.nspname AS schemaname, c.relname as sequencename, t.relname as tablename, a.attname as primaryKey "
					+ "FROM pg_class c " + "JOIN pg_namespace n ON n.oid = c.relnamespace "
					+ "JOIN pg_depend d ON d.objid = c.oid "
					+ "JOIN pg_class t ON d.objid = c.oid AND d.refobjid = t.oid "
					+ "JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum) "
					+ "WHERE c.relkind = 'S'";
			PreparedStatement statement = databaseConnect.getConnection().prepareStatement(sql);
			ResultSet resultSet = statement.executeQuery();
			if (resultSet != null) {
				while (resultSet.next()) {
					sequences.add(parseSequences(resultSet, exclusionSchemas, exclusionTables));
				}
			} else {
				LOGGER.fine("no sequences to reset");
			}
			statement.close();
		} catch (

		SQLException e) {
			throw new FusionException(new RequestException(e));
		}
		return sequences;
	}

	/**
	 * Reset the sequence to a specific value.
	 * 
	 * @param sequenceBDD
	 *            The sequence to reset.
	 * @param valueStart
	 *            The value to reset the sequence
	 * @throws FusionException
	 */
	private void setSequence(SequenceBDD sequenceBDD, String valueStart) throws FusionException {
		String sql = "SELECT setval('" + sequenceBDD.getSchemaNamePointSequenceName() + "', " + valueStart + ")";
		try (PreparedStatement statement = databaseConnect.getConnection().prepareStatement(sql)) {
			statement.executeQuery();
		} catch (SQLException e) {
			throw new FusionException(new RequestException(e));
		}
	}

	protected void vacuum() throws FusionException {
		LOGGER.info("VACUUM FULL");
		String vacuum = "VACUUM FULL";
		try (Statement statement = databaseConnect.getConnection().createStatement()) {
			statement.executeUpdate(vacuum);
		} catch (Exception e) {
			throw new FusionException("can't make a vacum full", e);
		}
	}

}
