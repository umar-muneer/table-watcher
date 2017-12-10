const mysql = require('mysql2');
const _ = require('lodash');
const util = require('util');
const createPool = () => {
	const {user, host, database, password, port} = options;
	return mysql.createPool({
		user: user,
		host: host,
		database: database,
		port: port
	});  
};
module.exports = ({database = '', watcher = '', options = {}} = {}) => {
	const pool = createPool(_.extend({database}, options)); 
	const query = util.promisify(pool.query);
	const watch = () => {
		const allTables = await pool.query(`use ${database}`);
		console.log(allTables, '@@@2');
	};
	return {watch};
};