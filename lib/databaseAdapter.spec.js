//const MongoClient = require('mongo-mock').MongoClient;

const DatabaseAdapter = require('./databaseAdapter').default;
const Users = require('./users');
const Collection = require('./collection');

const mongoUrl = 'mongodb://localhost:27017/gongoTest';

let FakeMongoClientWillThrow;
class FakeMongoClient {
  constructor(url) {
  }
  connect(callback) {
    if (FakeMongoClientWillThrow)
      callback(FakeMongoClientWillThrow);
    else
      callback(null, this);
  }
  db(name) {
    this.name = name;
    return this;
  }
}

describe('DatabaseAdapter', () => {

  describe('constructor', () => {

    it('sets instance vars', () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      expect(dba.client).toBeInstanceOf(FakeMongoClient);
      expect(dba.Users).toBeInstanceOf(Users);
    });

    // not worth testing MongoClient default
    it('has default params', async () => {
      const dba = new DatabaseAdapter(mongoUrl, undefined, FakeMongoClient);
      const client = await dba.dbPromise;//?
      expect((await dba.dbPromise).name).toBe('gongo');
    });

    it('sets promise to client connection', () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      return expect(dba.dbPromise).resolves.toBe(dba.client);
    });

    it('rejects errors from client connection', () => {
      FakeMongoClientWillThrow = new Error('error');
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      FakeMongoClientWillThrow = null;
      return expect(dba.dbPromise).rejects.toBeInstanceOf(Error);
    });

  }); /* constructor */

  describe('processChangeSet', () => {

    it('map correct funcs', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      const coll = dba.collection('test');

      const insertManyRV = {}, applyPatchesRV = {}, markAsDeletedRV = {};
      coll.insertMany = jest.fn().mockReturnValueOnce(Promise.resolve(insertManyRV));
      coll.applyPatches = jest.fn().mockReturnValueOnce(Promise.resolve(applyPatchesRV));
      coll.markAsDeleted = jest.fn().mockReturnValueOnce(Promise.resolve(markAsDeletedRV));

      const changeSet = {
        test: {
          insert: 'insert',
          update: 'update',
          delete: 'delete',
        }
      };

      const result = await dba.processChangeSet(changeSet, 'auth', 'req');

      expect(coll.insertMany).toHaveBeenCalledWith(changeSet.test.insert);
      expect(coll.applyPatches).toHaveBeenCalledWith(changeSet.test.update);
      expect(coll.markAsDeleted).toHaveBeenCalledWith(changeSet.test.delete);
    });

    it('does not call wrong funcs', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      const coll = dba.collection('test');

      coll.insertMany = jest.fn();
      coll.applyPatches = jest.fn();
      coll.markAsDeleted = jest.fn();

      await dba.processChangeSet({ test: {} });
      expect(coll.insertMany).not.toHaveBeenCalled();
      expect(coll.applyPatches).not.toHaveBeenCalled();
      expect(coll.markAsDeleted).not.toHaveBeenCalled();
    });


  }); /* procesChangeSet */

  describe('publishHelper', () => {

    it('appends updatedAt, returns toArray results in correct format', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      const coll = dba.collection('test');
      const cursor = coll.find({});

      const docs = [ { _id: 'id1' }, { _id: 'id2' } ];
      cursor.toArray = jest.fn();
      cursor.toArray.mockReturnValueOnce(Promise.resolve(docs));

      const updatedAt = { test: 'testUpdatedAt' };

      // results = [ { coll: 'test', entries: [ [Object], [Object] ] } ]
      const results = await dba.publishHelper(cursor, updatedAt, 'auth', 'req');

      expect(cursor.query.__updatedAt).toEqual({ $gt: updatedAt.test });

      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(1);

      const test = results[0];
      expect(test.coll).toBe('test');
      expect(test.entries.length).toBe(2);
      expect(test.entries[0]).toBe(docs[0]);
      expect(test.entries[1]).toBe(docs[1]);
    });

    it('works with empty query', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      const coll = dba.collection('test');
      const cursor = coll.find(); // <-- empty

      cursor.toArray = jest.fn();
      cursor.toArray.mockReturnValueOnce(Promise.resolve([]));

      const updatedAt = { test: 'testUpdatedAt' };
      const results = await dba.publishHelper(cursor, updatedAt, 'auth', 'req');
    });

    it('does not set updatedAt for irrelevant colls', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);
      const coll = dba.collection('test');
      const cursor = coll.find();

      cursor.toArray = jest.fn();
      cursor.toArray.mockReturnValueOnce(Promise.resolve([]));

      const updatedAt = { notTest: 'testUpdatedAt' };
      await dba.publishHelper(cursor, updatedAt, 'auth', 'req');
      expect(cursor.query.__updatedAt).not.toBeDefined();
    });

    it('returns same value for non-cursors', async () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);

      const obj = {}, arr = [];
      expect(await dba.publishHelper(obj)).toBe(obj);
      expect(await dba.publishHelper(arr)).toBe(arr);
      expect(await dba.publishHelper(1)).toBe(1);
      expect(await dba.publishHelper('str')).toBe('str');
    });

  }); /* procesChangeSet */

});
