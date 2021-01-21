//const MongoClient = require('mongo-mock').MongoClient;

const DatabaseAdapter = require('./databaseAdapter').default;

const mongoUrl = 'mongodb://localhost:27017/gongoTest';

/*
class TinyMockMongoClient {
  constructor() {
    const that = this;
    this.client = {
      connect() { return that; }
    }
  }
  collection() {
    return new TinyMockMongoCollection;
  }
}
*/

describe('DatabaseAdapter', () => {

  it('coming soon', () => {

  });

});
