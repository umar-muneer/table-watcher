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
	const host = process.env.DB_HOST || 'localhost';
	const user = process.env.DB_USER || 'root';
	const port = process.env.DB_PORT || 3306;
	const password = _.has(process.env, 'DB_PASSWORD') ? process.env.DB_PASSWORD : '';
	const database = DATABASE_NAME;
	const mysqldump = process.env.MYSQL_DUMP || 'mysqldump';
	before(async function() {		
		firstConnection = Promise.promisifyAll(mysql.createConnection({host, user}));
		await firstConnection.queryAsync(`create database if not exists ${DATABASE_NAME}`);		
		connection = Promise.promisifyAll(mysql.createConnection({host, user, database, multipleStatements: true}));
		await connection.queryAsync(`create table if not exists ${WATCH_TABLE_NAME} (table_name text, operation text, timestamp int);`);
		const testTableQueries = _.map(TEST_TABLE_NAMES, t => `create table if not exists ${t} (name text);`);
		await connection.queryAsync(testTableQueries.join(''));		
	});
	after(async function() {
		await firstConnection.execute('drop database test_table_watcher;');
	});
	beforeEach(async function() {
		await connection.queryAsync(`truncate test_table_watch;`);
	});
	describe('watch', () => {
		let watcher = {};
		before(async function() {
			watcher = Watcher(DATABASE_NAME, WATCH_TABLE_NAME, {host, user, port, password, mysqldump});
			await watcher.watch();
		});
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
		describe('watch with count map', () => {
			let watcherWithCount = {};
			before(async () => {
				watcherWithCount = Watcher(DATABASE_NAME, WATCH_TABLE_NAME, {host, user, port, password, mysqldump});
				const insertionQueries = _.map(TEST_TABLE_NAMES, t => `insert into ${t} values ('1');`);
				await connection.queryAsync(insertionQueries.join(''))
			});
			it('should generate counts map with watch', async function() {
				const expectedResult = {
					test: 1,
					test1: 1
				};
				const watch = await watcherWithCount.watch();
				const actualResult = watch.getTablesCountMap();
				chai.expect(actualResult).to.eql(expectedResult);
			});
			it('should get a list of truncated tables', async function() {
				const expectedResult = ['test1'];
				const watch = await watcherWithCount.watch();
				const query = `truncate test1`;
				await connection.queryAsync(query);
				const actualResult = await watch.getTruncatedTables();
				chai.expect(actualResult).to.eql(expectedResult);
			});
			it('should get a list of modified tables', async function() {
				const watch = await watcherWithCount.watch();
				const actualResult = await watch.getModifiedTables();
				chai.expect(TEST_TABLE_NAMES).to.eql(actualResult);
			});
			it('should create a restoration map for a list of tables', async function() {
				const watch = await watcherWithCount.watch();
				const actualResult = watch.getRestorationMap();
				chai.expect(actualResult['test']).to.not.be.empty;
				chai.expect(actualResult['test1']).to.not.be.empty;
			});
		});
	});
});