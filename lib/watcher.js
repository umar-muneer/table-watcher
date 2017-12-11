const _ = require('lodash');
const mysql = require('mysql2');
const Promise = require('bluebird');
const path = require('path');
const exec = require('child_process').exec;
const fs = require('fs');
const OPERATION_TYPES= {
	INSERT: 'INSERT',
	UPDATE: 'UPDATE',
	DELETE: 'DELETE'
};
module.exports = (database = '', watcher = '', options = {}) => {
	if (!database || !watcher) {
		throw new Error('database or watcher table name is not provided');
	}
	const {host, user, password, port, mysqldump, restorationDumpsFolder} = options;
	if (!mysqldump) {
		throw new Error('you need to provide path to mysql binaries');
	}
	if (!restorationDumpsFolder) {
		throw new Error('you need to provide a folder to save the restoration dumps');
	}
	const connection = Promise.promisifyAll(mysql.createConnection(_.extend({
		database: database,
		multipleStatements: true
	}, {host, user, password, port})));
	const getTriggerQueries = (table) => {		
		const triggerTexts = _.map(OPERATION_TYPES, ot => {
			return `
				DROP TRIGGER IF EXISTS ${table}_${ot};
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
	const createTriggers = async (tables = []) => {
		const triggerQueries = _.map(tables, table => {
			return getTriggerQueries(table);
		});		
		await connection.queryAsync(triggerQueries.join('\n'));
	};
	const getAllTables = async () => {
		const query = "show full tables where Table_Type = 'BASE TABLE'";
		const result = await connection.queryAsync(query);
		return _.chain(result)
						.map(row => row[`Tables_in_${database}`])
						.filter(table => table !== watcher)
						.value();
	};
	const getCountQuery = (tables = []) => {
		 return _.chain(tables)
		 	.map(table => {
				return `(select count(*) as count, '${table}' as tableName from ${table})`;
			})
			.join(' union all\n')
			.value();
	};
	const createTablesCountMap = async (tables, condition = (el) => el.count > 0) => {
		const query = getCountQuery(tables);
		const result = await connection.queryAsync(query);
		return _.chain(result)
						.filter(condition)
						.reduce((acc, row) => {
							acc[row.tableName] = row.count;
							return acc;
						}, {})
						.value();
	};
	const createRestorationMap = async (tables) => {
		const map = {};
		return await Promise.reduce(tables, (acc, table) => {
			const dumpPath = path.join(restorationDumpsFolder, table);
			const command = `${mysqldump} -h ${host} -P ${port} -u ${user} --password=${password} --skip-comments --add-drop-table ${database} ${table}>${dumpPath}`;
			return new Promise((resolve, reject) => {
				exec(command, {maxBuffer: 1024 * 5000}, function(error, stdout) {
					if (error) {
						reject(error);
						return;
					}
					resolve(stdout);
				});	
			}).then((result) => {
				acc[table] = dumpPath;
				return acc;
			});
		}, {});
	};
	const watch = async () => {
		const tablesToWatch = await getAllTables();				
		const map = await createTablesCountMap(tablesToWatch, el => el.count > 0);		
		await createTriggers(tablesToWatch);
		const restorationMap = await createRestorationMap(_.keys(map));
		const getTruncatedTables = async () => {
			const tables = _.keys(map);
			const newMap = await createTablesCountMap(tables, el => el.count === 0);
			return _.chain(newMap)
							.keys()
							.reject(key => newMap[key].count === 0)
							.value();
		};
		const getTablesCountMap = () => map;		
		const getModifiedTables = async () => {
			const query = `select distinct table_name from ${watcher}`;
			const result = await connection.queryAsync(query);
			return _.map(result, 'table_name');
		};
		const getRestorationMap = () => restorationMap;		
		return {getTruncatedTables, getTablesCountMap, getModifiedTables, getRestorationMap};
	};
	return {watch, OPERATION_TYPES};
};