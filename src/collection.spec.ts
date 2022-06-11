import Collection from "./collection";
import Cursor from "./cursor";

const now = Date.now();
const dateNowSpy = jest.spyOn(global.Date, "now");
function dateNowNext(value = now) {
  dateNowSpy.mockImplementationOnce(() => value);
}

describe("Collection", () => {
  describe("constructor", () => {
    it("stores instance vars", () => {
      // @ts-expect-error: stub
      const col = new Collection("db", "name");
      expect(col.db).toBe("db");
      expect(col.name).toBe("name");
    });
  });

  describe("getReal", () => {
    it("returns the mongo collection", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn() };

      mongoDb.collection.mockReturnValueOnce(mongoCol);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      expect(await collection.getReal()).toBe(mongoCol);
    });

    it("calls createIndex once -- for now XXX TODO", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn() };

      mongoDb.collection.mockReturnValueOnce(mongoCol);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      expect(mongoCol.createIndex.mock.calls.length).toBe(0);
      await collection.getReal();
      expect(mongoCol.createIndex.mock.calls.length).toBe(1);
      await collection.getReal();
      expect(mongoCol.createIndex.mock.calls.length).toBe(1);
    });
  });

  describe("find", () => {
    it("returns a cursor for this col & query", () => {
      const db = {};
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");
      // @ts-expect-error: stub
      const cursor = collection.find("filter");

      expect(cursor.db).toBe(db);
      expect(cursor.coll).toBe(collection);
      expect(cursor.filter).toBe("filter");
      expect(cursor).toBeInstanceOf(Cursor);
    });
  });

  describe("findOne", () => {
    it("should call real findOne with same query", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), findOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.findOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");
      // @ts-expect-error: stub
      const result = collection.findOne("query");

      expect(await result).toBe(mongoResult);
      expect(mongoCol.findOne).toHaveBeenCalledWith("query");
    });
  });

  describe("insertOne", () => {
    it("should add updatedAt and call mongo insertOne", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), insertOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.insertOne.mockReturnValueOnce(mongoResult);

      const newRow = { _id: "a" };
      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      dateNowNext(now);
      const result = await collection.insertOne(newRow);
      expect(result).toBe(mongoResult);
      expect(mongoCol.insertOne).toHaveBeenCalledWith({
        _id: "a",
        __updatedAt: now,
      });
    });
  });

  describe("insertMany", () => {
    it("adds __updatedAt to all docs", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), insertMany: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.insertMany.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      // For now we'll be lazy and rely on fact that API says we can mutate
      const docs = [{ _id: "a" }, { _id: "b" }];
      await collection.insertMany(docs);
      // @ts-expect-error: stub
      expect(docs[0].__updatedAt).toBeDefined();
      // @ts-expect-error: stub
      expect(docs[1].__updatedAt).toBeDefined();
    });

    /*
    it("should bulkWrite to upsert", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), bulkWrite: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const docs = [{ _id: "a" }, { _id: "b" }];
      await collection.insertMany(docs);

      expect(mongoCol.bulkWrite).toHaveBeenCalledWith([
        {
          replaceOne: {
            filter: { _id: "a" },
            //update: { $setOnInsert: docs[0] },
            replacement: docs[0],
            upsert: true,
          },
        },
        {
          replaceOne: {
            filter: { _id: "b" },
            // update: { $setOnInsert: docs[1] },
            replacement: docs[1],
            upsert: true,
          },
        },
      ]);
    });
    */
  });

  describe("markAsDeleted", () => {
    it("given array of ids, bulkwrite replace with __deleted, __updatedAt", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), bulkWrite: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      dateNowNext(now);
      await collection.markAsDeleted(["a", "b"]);

      expect(mongoCol.bulkWrite).toHaveBeenCalledWith([
        {
          replaceOne: {
            filter: { _id: "a" },
            replacement: { _id: "a", __deleted: true, __updatedAt: now },
            upsert: true,
          },
        },
        {
          replaceOne: {
            filter: { _id: "b" },
            replacement: { _id: "b", __deleted: true, __updatedAt: now },
            upsert: true,
          },
        },
      ]);
    });
  });

  describe("replaceOne", () => {
    it("relay to mongo replaceOne", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), replaceOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.replaceOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const doc = {};
      // @ts-expect-error: stub
      await collection.replaceOne("query", doc, "options");
      expect(mongoCol.replaceOne).toHaveBeenCalledWith("query", doc, "options");
    });

    it("sets __updatedAt on replaced doc", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), replaceOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.replaceOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      dateNowNext(now);
      const doc = {};
      // @ts-expect-error: stub
      await collection.replaceOne("query", doc, "options");
      // @ts-expect-error: stub
      expect(doc.__updatedAt).toBe(now);
    });

    it("throws on falsy doc", () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), replaceOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.replaceOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      return expect(
        // @ts-expect-error: purposefully testing bad runtime values
        collection.replaceOne("queryA" /* no doc */)
      ).rejects.toBeInstanceOf(Error);
    });
  });

  describe("updateOne", () => {
    it("relay to mongo updateOne", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), updateOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const doc = {};
      // @ts-expect-error: stub
      await collection.updateOne("query", doc, "options");
      expect(mongoCol.updateOne).toHaveBeenCalledWith("query", doc, "options");
    });

    it("sets __updatedAt on updated doc", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), updateOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      dateNowNext(now);
      const update = {};
      // @ts-expect-error: stub
      await collection.updateOne("query", update, "options");
      // @ts-expect-error: stub
      expect(update.$set.__updatedAt).toBe(now);
    });

    it("will work even with empty update (and update updatedAt)", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { createIndex: jest.fn(), updateOne: jest.fn() };
      const mongoResult = {};

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      dateNowNext(now);
      // @ts-expect-error: stub
      await collection.updateOne("query" /* doc, options */);
      expect(mongoCol.updateOne).toHaveBeenCalledWith("query", {
        $set: {
          __updatedAt: now,
        },
      });
    });
  });

  describe("applyPatch", () => {
    it("converts patch to mongo and updatesOnes", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = {
        createIndex: jest.fn(),
        updateOne: jest.fn(),
        findOne: jest.fn(),
      };
      const mongoResult = {};

      mongoDb.collection.mockReturnValue(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const origDoc = { _id: "id", a: 1 };
      mongoCol.findOne.mockReturnValueOnce(origDoc);

      const entry = {
        _id: "id",
        patch: [{ op: "replace", path: "/a", value: 2 }],
      };

      dateNowNext(now);
      // @ts-expect-error: stub
      await collection.applyPatch(entry);
      expect(mongoCol.updateOne).toBeCalledWith(
        { _id: "id" },
        {
          $set: {
            a: 2,
            __updatedAt: now,
          },
        }
      );
    });
  });

  describe("applyPatches", () => {
    it("converts patch to mongo and updatesOnes", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = {
        createIndex: jest.fn(),
        bulkWrite: jest.fn(),
        find: jest.fn(),
      };
      const mongoResult = {};

      mongoDb.collection.mockReturnValue(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const origDocs = [
        { _id: "id1", a: 1 },
        { _id: "id2", a: 2 },
      ];
      const entries = [
        {
          _id: "id1",
          patch: [{ op: "replace", path: "/a", value: 3 }],
        },
        {
          _id: "id2",
          patch: [{ op: "replace", path: "/a", value: 4 }],
        },
      ];

      const toArray = jest.fn();
      mongoCol.find.mockReturnValueOnce({ toArray });
      toArray.mockReturnValueOnce(origDocs);

      dateNowNext(now);
      dateNowNext(now);
      // @ts-expect-error: stub
      await collection.applyPatches(entries);
      expect(mongoCol.bulkWrite).toBeCalledWith([
        {
          updateOne: {
            filter: { _id: "id1" },
            update: { $set: { a: 3, __updatedAt: now } },
          },
        },
        {
          updateOne: {
            filter: { _id: "id2" },
            update: { $set: { a: 4, __updatedAt: now } },
          },
        },
      ]);
    });

    it("handles case where toMongoDb does not create $set", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = {
        createIndex: jest.fn(),
        bulkWrite: jest.fn(),
        find: jest.fn(),
      };
      const mongoResult = {};

      mongoDb.collection.mockReturnValue(mongoCol);
      mongoCol.bulkWrite.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const origDocs = [{ _id: "id1", a: 1 }];
      const entries = [
        {
          _id: "id1",
          patch: [],
        },
      ];

      const toArray = jest.fn();
      mongoCol.find.mockReturnValueOnce({ toArray });
      toArray.mockReturnValueOnce(origDocs);

      dateNowNext(now);
      await collection.applyPatches(entries);
      expect(mongoCol.bulkWrite).toBeCalledWith([
        {
          updateOne: {
            filter: { _id: "id1" },
            update: { $set: { __updatedAt: now } },
          },
        },
      ]);
    });
  });
});
