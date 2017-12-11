const _ = require('lodash');
const OPERATION_TYPES= {
	INSERT: 'INSERT',
	UPDATE: 'UPDATE',
	DELETE: 'DELETE'
};
module.exports = (database = '', watcher = '') => {
	if (!database || !watcher) {
		throw new Error('database or watcher table name is not provided');
	}
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
		return triggerTexts.join('')
	};
	const watch = async (connection) => {
		const result = await connection.queryAsync('show tables');
		const tablesToWatch = _.filter(result, row => row[`Tables_in_${database}`] !== watcher);
		const triggerQueries = _.map(tablesToWatch, row => {
			return getTriggerQueries(row['Tables_in_test_table_watcher']);
		});
		return await connection.queryAsync(triggerQueries.join('\n'));
	};
	return {watch, OPERATION_TYPES};
};