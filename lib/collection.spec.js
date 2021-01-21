const Collection = require('./collection');
const Cursor = require('./cursor');

const now = Date.now();
const dateNowSpy = jest.spyOn(global.Date, 'now');
function dateNowNext(value = 'now') {
  dateNowSpy.mockImplementationOnce(() => value)
}

describe('Collection', () => {

  describe('constructor', () => {

    it('stores instance vars', () => {
      const col = new Collection('db', 'name');
      expect(col.db).toBe('db');
      expect(col.name).toBe('name');
    });

  });

  describe('getReal', () => {

    it('returns the mongo collection', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn() };

      mongoDb.collection.mockReturnValueOnce(mongoCol);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');

      expect(await collection.getReal()).toBe(mongoCol);
    });

    it('calls createIndex once -- for now XXX TODO', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn() };

      mongoDb.collection.mockReturnValueOnce(mongoCol);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');

      expect(mongoCol.createIndex.mock.calls.length).toBe(0);
      await collection.getReal();
      expect(mongoCol.createIndex.mock.calls.length).toBe(1);
      await collection.getReal();
      expect(mongoCol.createIndex.mock.calls.length).toBe(1);
    });

  });

  describe('find', () => {

    it('returns a cursor for this col & query', () => {
      const db = {};
      const collection = new Collection(db, 'collection');
      const cursor = collection.find('query');

      expect(cursor.db).toBe(db);
      expect(cursor.coll).toBe(collection);
      expect(cursor.query).toBe('query');
      expect(cursor).toBeInstanceOf(Cursor);
    });

  });

  describe('findOne', () => {

    it('should call real findOne with same query', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), findOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.findOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');
      const result = collection.findOne('query');

      expect(await result).toBe(mongoResult);
      expect(mongoCol.findOne).toHaveBeenCalledWith('query');
    });

  });

  describe('insertOne', () => {

    it('should add updatedAt and call mongo insertOne', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), insertOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.insertOne.mockReturnValueOnce(mongoResult);

      const newRow = { _id: 'a' };
      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');

      dateNowNext('now');
      const result = await collection.insertOne(newRow);
      expect(result).toBe(mongoResult);
      expect(mongoCol.insertOne).toHaveBeenCalledWith({
        _id: 'a',
        __updatedAt: 'now'
      })
    });

  });

  describe('insertMany', () => {

    it('adds __updatedAt to all docs', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), bulkWrite: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');

      // For now we'll be lazy and rely on fact that API says we can mutate
      const docs = [ { _id: 'a' }, { _id: 'b'} ];
      await collection.insertMany(docs);
      expect(docs[0].__updatedAt).toBeDefined();
      expect(docs[1].__updatedAt).toBeDefined();
    });


    it('should bulkWrite to upsert', async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), bulkWrite: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const collection = new Collection(db, 'collection');

      // For now we'll be lazy and rely on fact that API says we can mutate
      const docs = [ { _id: 'a' }, { _id: 'b'} ];
      await collection.insertMany(docs);

      expect(mongoCol.bulkWrite).toHaveBeenCalledWith([
        {
          replaceOne: {
            filter: { _id: 'a' },
            update: { $setOnInsert: docs[0] },
            upsert: true,
          }
        },
        {
          replaceOne: {
            filter: { _id: 'b' },
            update: { $setOnInsert: docs[1] },
            upsert: true,
          }
        }
      ]);
    });

  });


});
