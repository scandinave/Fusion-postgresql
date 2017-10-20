/**
 * 
 */
package info.scandi.fusion.database;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import info.scandi.fusion.core.ConfigurationManager;
import info.scandi.fusion.database.bdd.TableBDD;
import info.scandi.fusion.exception.FusionException;
import info.scandi.fusion.utils.FileUtil;

/**
 * Saves the data of the database in xml files. One file by datatable.
 * Tables can be exclude from the generation.
 * 
 * @author Scandinave
 */
@Named
@ApplicationScoped
public class Saver implements Serializable {

	/**
	 * serialVersionUID long.
	 */
	private static final long serialVersionUID = 142577290372523301L;

	@Inject
	private Logger LOGGER;

	@Inject
	private IDatabaseConnection databaseConnect;

	@Inject
	private ConfigurationManager conf;

	@Inject
	DatabaseProducer databaseProducer;

	/**
	 * List of tables to save.
	 */
	private Set<TableBDD> tables;

	/**
	 * Starts the save operation.
	 * 
	 * @param set
	 * 
	 * @throws FusionException
	 */
	public void start(Set<TableBDD> tables) throws FusionException {
		cleanPreviousBackupFile();
		exportDatabaseToXml(tables);
	}

	/**
	 * Deletes all xml file that was used for the last save.
	 */
	private void cleanPreviousBackupFile() {
		FileUtil.cleanDirectories(conf.getBackupDirectory());
	}

	/**
	 * Writes the data into xml files.
	 * 
	 * @throws FusionException
	 */
	private void exportDatabaseToXml(Set<TableBDD> tables) throws FusionException {
		LOGGER.info("Début de la sauvegarde de la base de données");
		FileOutputStream outputStream = null;
		try {
			for (TableBDD table : tables) {
				File file = new File(
						conf.getBackupDirectory() + "/" + table.getSchemaName() + "." + table.getTableName() + ".xml");
				File parent = file.getParentFile();
				if (!file.getParentFile().exists()) {
					parent.mkdirs();
				}
				QueryDataSet dataSetParTable = new QueryDataSet(databaseConnect);
				dataSetParTable.addTable(table.getSchemaName() + "." + table.getTableName());
				outputStream = new FileOutputStream(file);
				FlatXmlDataSet.write(dataSetParTable, outputStream);
				LOGGER.fine(file.getCanonicalPath() + " saved");
				outputStream.close();
			}
		} catch (AmbiguousTableNameException e) {
			throw new FusionException(e);
		} catch (FileNotFoundException e1) {
			throw new FusionException(e1);
		} catch (DataSetException | IOException e2) {
			throw new FusionException(e2);
		} finally {
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					throw new FusionException(e);
				}
			}
		}
		LOGGER.info("Backup completed successfully.");
	}
}
