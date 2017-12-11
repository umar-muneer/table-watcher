const _ = require('lodash');
const mysql = require('mysql2');
const Promise = require('bluebird');
const OPERATION_TYPES= {
	INSERT: 'INSERT',
	UPDATE: 'UPDATE',
	DELETE: 'DELETE'
};
module.exports = (database = '', watcher = '', options = {}) => {
	if (!database || !watcher) {
		throw new Error('database or watcher table name is not provided');
	}
	const {host, user, password, port} = options;
	const connection = Promise.promisifyAll(mysql.createConnection(_.extend({
		database: database,
		multipleStatements: true
	}, {host, user, password, port})));
	const getTriggerQueries = (table) => {		
		const triggerTexts = _.map(OPERATION_TYPES, ot => {
			return `
				CREATE TRIGGER ${table}_${ot}
						BEFORE ${ot} ON ${table}
						FOR EACH ROW 
				BEGIN
						INSERT INTO ${watcher}
						SET table_name = '${table}',
						operation = '${ot}',
						timestamp = UNIX_TIMESTAMP();
				END;
				`;
		});		
		return triggerTexts.join('');
	};
	const watch = async () => {
		const result = await connection.queryAsync("show full tables where Table_Type = 'BASE TABLE'");
		const tablesToWatch = _.filter(result, row => row[`Tables_in_${database}`] !== watcher);
		const triggerQueries = _.map(tablesToWatch, row => {
			return getTriggerQueries(row[`Tables_in_${database}`]);
		});
		return await connection.queryAsync(triggerQueries.join('\n'));
	};
	return {watch, OPERATION_TYPES};
};