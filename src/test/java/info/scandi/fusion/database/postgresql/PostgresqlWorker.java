package info.scandi.fusion.database.postgresql;

import javax.enterprise.inject.Alternative;

import info.scandi.fusion.exception.FusionException;

@info.scandi.fusion.utils.Worker
@Alternative
public class PostgresqlWorker extends AbstractPostgresqlWorker {

	protected PostgresqlWorker() throws FusionException {
		super();
		// TODO Auto-generated constructor stub
	}

}
