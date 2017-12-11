const chai = require('chai');
const _ = require('lodash');
const mysql = require('mysql2');
const Promise = require('bluebird');
const Watcher = require('../index');
const DATABASE_NAME = 'test_table_watcher';
const WATCH_TABLE_NAME = 'test_table_watch';
const TEST_TABLE_NAMES = ['test', 'test1'];
describe('watcher', () => {
	let firstConnection = {};
	let connection = {};
	let watcher = {};
	before(async function() {
		const host= process.env.DB_HOST || 'localhost1';
		const user = process.env.DB_USER || 'root1';
		const port = process.env.DB_PORT || 33061;
		const password = _.has(process.env, 'DB_PASSWORD') ? process.env.DB_PASSWORD : '';
		const database = DATABASE_NAME;
		firstConnection = Promise.promisifyAll(mysql.createConnection({host, user}));
		await firstConnection.queryAsync(`create database if not exists ${DATABASE_NAME}`);		
		connection = Promise.promisifyAll(mysql.createConnection({host, user, database, multipleStatements: true}));
		await connection.queryAsync(`create table if not exists ${WATCH_TABLE_NAME} (table_name text, operation text, timestamp int);`);
		const testTableQueries = _.map(TEST_TABLE_NAMES, t => `create table if not exists ${t} (name text);`);
		await connection.queryAsync(testTableQueries.join(''));
		watcher = Watcher(DATABASE_NAME, WATCH_TABLE_NAME, {host, user, port, password});
		await watcher.watch();
	});
	after(async function() {
		await firstConnection.execute('drop database test_table_watcher;');
	});
	beforeEach(async function() {
		await connection.queryAsync(`truncate test_table_watch;`);
	});
	describe('watch', () => {
		beforeEach(async function() {						
			const truncationQueries = _.map(TEST_TABLE_NAMES, t => `truncate ${t};`);
			await connection.queryAsync(truncationQueries.join(''));
			const insertionQueries = _.map(TEST_TABLE_NAMES, t => `insert into ${t} values ('1');`);
			await connection.queryAsync(insertionQueries.join(''));			
		});
		it('should wacth tables if data is inserted in the tables', async function() {
			const select = `select * from ${WATCH_TABLE_NAME} where table_name in ('test', 'test1') and operation = '${watcher.OPERATION_TYPES.INSERT}'`;
			const actualResult = await connection.queryAsync(select);
			chai.expect(actualResult.length).to.equal(2);
		});
		it('should watch tables if data is updated in the tables', async function() {
			const updateQueries = _.map(TEST_TABLE_NAMES, t => `update ${t} set name = '2';`);
			await connection.queryAsync(updateQueries.join(''));
			const select = `select distinct(table_name) from ${WATCH_TABLE_NAME} where table_name in ('test', 'test1') and operation = '${watcher.OPERATION_TYPES.UPDATE}'`;
			const actualResult = await connection.queryAsync(select);
			chai.expect(actualResult.length).to.equal(2);
		});
		it('should watch tables if data is deleted from the tables', async function() {
			const deletionQueries = _.map(TEST_TABLE_NAMES, t => `delete from ${t};`);
			await connection.queryAsync(deletionQueries.join(''));
			const select = `select distinct(table_name) from ${WATCH_TABLE_NAME} where table_name in ('test', 'test1') and operation = '${watcher.OPERATION_TYPES.DELETE}'`;
			const actualResult = await connection.queryAsync(select);
			chai.expect(actualResult.length).to.equal(2);
		});
	});
});