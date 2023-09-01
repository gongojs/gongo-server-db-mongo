import { ObjectId } from "bson";
import Collection, { GongoDocument } from "./collection";
import Cursor from "./cursor";
import type { OpError } from "gongo-server/lib/DatabaseAdapter.js";
import "./toHaveObjectId";

const now = Date.now();
const dateNowSpy = jest.spyOn(global.Date, "now");
function dateNowNext(value = now) {
  dateNowSpy.mockImplementationOnce(() => value);
}

interface DocWithOptStrId extends GongoDocument {
  _id?: string;
}

const mkId = () => new ObjectId().toHexString();
const mkIds = (i: number) =>
  "X"
    .repeat(i)
    .split("")
    .map(() => new ObjectId().toHexString());

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
      const collection = new Collection<DocWithOptStrId>(db, "collection");

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
      const collection = new Collection<DocWithOptStrId>(db, "collection");

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
      await collection.markAsDeleted([
        "12345678901234567890aaaa",
        "12345678901234567890bbbb",
      ]);

      expect(mongoCol.bulkWrite).toHaveBeenCalledWith([
        {
          replaceOne: {
            filter: { _id: expect.toHaveObjectId("12345678901234567890aaaa") },
            replacement: { __deleted: true, __updatedAt: now },
            upsert: true,
          },
        },
        {
          replaceOne: {
            filter: { _id: expect.toHaveObjectId("12345678901234567890bbbb") },
            replacement: { __deleted: true, __updatedAt: now },
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

      const origDoc = { _id: "12345678901234567890aaaa", a: 1 };
      mongoCol.findOne.mockReturnValueOnce(origDoc);

      const entry = {
        _id: "12345678901234567890aaaa",
        patch: [{ op: "replace", path: "/a", value: 2 }],
      };

      dateNowNext(now);
      // @ts-expect-error: stub
      await collection.applyPatch(entry);
      expect(mongoCol.updateOne).toBeCalledWith(
        { _id: expect.toHaveObjectId("12345678901234567890aaaa") },
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
    /*
    it("converts patch to mongo and bulkUpdates", async () => {
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

      const [id1, id2] = mkIds(2);

      const origDocs = [
        { _id: id1, a: 1 },
        { _id: id2, a: 2 },
      ];
      const entries = [
        {
          _id: id1,
          patch: [{ op: "replace", path: "/a", value: 3 }],
        },
        {
          _id: id2,
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
            filter: { _id: expect.toHaveObjectId(id1) },
            update: { $set: { a: 3, __updatedAt: now } },
          },
        },
        {
          updateOne: {
            filter: { _id: expect.toHaveObjectId(id2) },
            update: { $set: { a: 4, __updatedAt: now } },
          },
        },
      ]);
    });
    */

    it("converts patch to mongo and updatesOnes", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = {
        createIndex: jest.fn(),
        updateOne: jest.fn(),
        find: jest.fn(),
        findOne: jest.fn(),
        getReal: jest.fn(),
      };
      const mongoResult = {};

      mongoDb.collection.mockReturnValue(mongoCol);
      mongoCol.getReal.mockReturnValue(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const [id1, id2] = mkIds(2);

      const origDocs = [
        { _id: id1, a: 1 },
        { _id: id2, a: 2 },
      ];
      const entries = [
        {
          _id: id1,
          patch: [{ op: "replace", path: "/a", value: 3 }],
        },
        {
          _id: id2,
          patch: [{ op: "replace", path: "/a", value: 4 }],
        },
      ];

      // const toArray = jest.fn();
      // mongoCol.find.mockReturnValueOnce({ toArray });
      // toArray.mockReturnValueOnce(origDocs);

      mongoCol.findOne.mockReturnValueOnce(origDocs[0]);
      mongoCol.findOne.mockReturnValueOnce(origDocs[1]);

      dateNowNext(now);
      dateNowNext(now);
      // @ts-expect-error: stub
      await collection.applyPatches(entries);

      expect(mongoCol.updateOne).toHaveBeenCalledTimes(2);
      expect(mongoCol.updateOne).toBeCalledWith(
        { _id: expect.toHaveObjectId(id1) },
        {
          $set: { __updatedAt: expect.any(Number), a: 3 },
        }
      );
      expect(mongoCol.updateOne).toBeCalledWith(
        { _id: expect.toHaveObjectId(id2) },
        {
          $set: { __updatedAt: expect.any(Number), a: 4 },
        }
      );
    });

    /*
    it("handles case where toMongoDb does not create $set (bulkwrite)", async () => {
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

      const id1 = mkId();
      const origDocs = [{ _id: id1, a: 1 }];
      const entries = [
        {
          _id: id1,
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
            filter: { _id: expect.toHaveObjectId(id1) },
            update: { $set: { __updatedAt: now } },
          },
        },
      ]);
    });
  });
  */

    it("handles case where toMongoDb does not create $set (updateOnes)", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = {
        createIndex: jest.fn(),
        updateOne: jest.fn(),
        find: jest.fn(),
        findOne: jest.fn(),
        getReal: jest.fn(),
      };
      const mongoResult = {};

      mongoDb.collection.mockReturnValue(mongoCol);
      mongoCol.getReal.mockReturnValue(mongoCol);
      mongoCol.updateOne.mockReturnValueOnce(mongoResult);

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const collection = new Collection(db, "collection");

      const id1 = mkId();
      const origDocs = [{ _id: id1, a: 1 }];
      const entries = [
        {
          _id: id1,
          patch: [],
        },
      ];

      const toArray = jest.fn();
      mongoCol.find.mockReturnValueOnce({ toArray });
      toArray.mockReturnValueOnce(origDocs);

      dateNowNext(now);
      await collection.applyPatches(entries);

      expect(mongoCol.updateOne).toHaveBeenCalledTimes(1);
      expect(mongoCol.updateOne).toBeCalledWith(
        { _id: expect.toHaveObjectId(id1) },
        {
          $set: { __updatedAt: now },
        }
      );
    });
  });

  describe("events", () => {
    describe("on", () => {
      it("set event", () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        const func = function () {};
        col.on("preInsertMany", func);
        expect(col._events.preInsertMany[0]).toBe(func);
      });

      it("throws on non-existent event name", () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        const func = function () {};
        // @ts-expect-error: exactly what we're testing for
        expect(() => col.on("DOES_NOT_EXIST", func)).toThrow(/No such event/);
      });
    });

    describe("eventExec", () => {
      it("runs event", () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        const func = jest.fn();
        col.on("preInsertMany", func);
        // @ts-expect-error: stub
        col.eventExec("preInsertMany");
        expect(func).toHaveBeenCalledTimes(1);
      });

      it("throws on no such event", () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // @ts-expect-error: stub
        const func = () => col.eventExec("DOES_NOT_EXIST");
        expect(func).rejects.toThrow(/No such event/);
      });
    });
  });

  describe("allows", () => {
    describe("insert", () => {
      /*
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // @ts-expect-error: stub
        expect(() => col.allowFilter("insert", [], {})).toThrowError(
          /has no allow handler/
        );
      });
      */
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        const errors: Array<OpError> = [];
        const docs = [{ _id: "a" }, { _id: "b" }];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("insert", docs, {}, errors);

        expect(filtered).toHaveLength(0);
        expect(errors).toHaveLength(2);
        expect(errors[0]).toMatchObject(["a", 'No "insert" allow handler']);
      });

      it("allows only allowed inserts", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        col.allow("insert", async (doc /* _props */) => {
          return !!doc.allowed;
        });
        const errors: OpError[] = [];
        const docs = [{ notAllowed: true }, { allowed: true }];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("insert", docs, {}, errors);

        expect(filtered.length).toBe(1);
        expect(filtered[0]).toMatchObject({ allowed: true });
      });
    });

    describe("update", () => {
      /*
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // @ts-expect-error: stub
        expect(() => col.allowFilter("update", [], {})).toThrowError(
          /has no allow handler/
        );
      });
      */
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        const errors: Array<OpError> = [];
        const docs = [{ _id: "a" }, { _id: "b" }];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("update", docs, {}, errors);

        expect(filtered).toHaveLength(0);
        expect(errors).toHaveLength(2);
        expect(errors[0]).toMatchObject(["a", 'No "update" allow handler']);
      });

      it("allows only allowed updates", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        col.allow("update", async (changeSet /*, props */) => {
          return changeSet._id === "stay";
        });
        const errors: OpError[] = [];
        const docs = [
          { _id: "stay", patch: [] },
          { _id: "go", patch: [] },
        ];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("update", docs, {}, errors);

        expect(filtered.length).toBe(1);
        expect(filtered[0]).toMatchObject({ _id: "stay" });
      });
    });

    describe("remove", () => {
      /*
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        // @ts-expect-error: stub
        expect(() => col.allowFilter("remove", [], {})).toThrowError(
          /has no allow handler/
        );
      });
      */
      it("blocks by default", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        const errors: Array<OpError> = [];
        const docs = ["a", "b"];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("remove", docs, {}, errors);

        expect(filtered).toHaveLength(0);
        expect(errors).toHaveLength(2);
        expect(errors[0]).toMatchObject(["a", 'No "remove" allow handler']);
      });

      it("allows only allowed removes", async () => {
        // @ts-expect-error: stub
        const col = new Collection("db", "name");
        col.allow("remove", async (id /*, props */) => {
          return id === "stay";
        });
        const errors: OpError[] = [];
        const docIds = ["stay", "go"];
        // @ts-expect-error: stub
        const filtered = await col.allowFilter("remove", docIds, {}, errors);

        expect(filtered.length).toBe(1);
        expect(filtered[0]).toBe("stay");
      });
    });
  });
});
