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
  db() {
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

  });

  describe('collection', () => {

    it('creates or returns collection', () => {
      const dba = new DatabaseAdapter(mongoUrl, 'gongo', FakeMongoClient);

      const firstCall = dba.collection('name');
      expect(firstCall).toBeInstanceOf(Collection);

      const secondCall = dba.collection('name');
      expect(secondCall).toBe(firstCall);
    });

  });

});
