import { jest } from "@jest/globals";
import { ObjectId } from "mongodb";
import type { MongoClient } from "mongodb";

import MongoDBA from "./databaseAdapter";
import Users from "./users";
import type Collection from "./collection";
import type Cursor from "./cursor";
import type GongoServerless from "gongo-server/lib/serverless.js";
import type { PublicationProps } from "gongo-server/lib/publications.js";

const mongoUrl = "mongodb://localhost:27017/gongoTest";

let FakeMongoClientWillThrow: Error | null = null;
class _FakeMongoClient {
  name = "";
  connect(callback: (err: unknown, res?: unknown) => void) {
    if (FakeMongoClientWillThrow) callback(FakeMongoClientWillThrow);
    else callback(null, this);
  }
  db(name: string) {
    this.name = name;
    return this;
  }
}
const FakeMongoClient = _FakeMongoClient as unknown as typeof MongoClient;

describe("MongoDBA", () => {
  describe("constructor", () => {
    it("sets instance vars", () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      expect(dba.client).toBeInstanceOf(FakeMongoClient);
      expect(dba.Users).toBeInstanceOf(Users);
    });

    // not worth testing MongoClient default
    it("has default params", async () => {
      const dba = new MongoDBA(mongoUrl, undefined, FakeMongoClient);
      /* const client = */ await dba.dbPromise; //?
      // @ts-expect-error: stub
      expect((await dba.dbPromise).name).toBe("gongo");
    });

    it("sets promise to client connection", () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      return expect(dba.dbPromise).resolves.toBe(dba.client);
    });

    it("rejects errors from client connection", () => {
      FakeMongoClientWillThrow = new Error("error");
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      FakeMongoClientWillThrow = null;
      return expect(dba.dbPromise).rejects.toBeInstanceOf(Error);
    });
  }); /* constructor */

  describe("onInit", () => {
    describe("arson", () => {
      it("registers type", () => {
        const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
        const gs = {
          ARSON: { registerType: jest.fn() },
        } as GongoServerless<MongoDBA>;
        dba.onInit(gs);

        expect(gs.ARSON.registerType.mock.calls[0][0]).toBe("ObjectID");

        const { deconstruct, reconstruct } =
          gs.ARSON.registerType.mock.calls[0][1];

        let oid = ObjectId.createFromHexString("aaaaaaaaaaaaaaaaaaaaaaaa");
        expect(deconstruct(oid)).toEqual(["aaaaaaaaaaaaaaaaaaaaaaaa"]);

        oid = reconstruct(["aaaaaaaaaaaaaaaaaaaaaaaa"]);
        expect(oid.toHexString()).toBe("aaaaaaaaaaaaaaaaaaaaaaaa");
      });
    });
  });

  describe("processChangeSet", () => {
    it("map correct funcs", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      const coll = dba.collection("test");

      /*
      const insertManyRV = {},
        applyPatchesRV = {},
        markAsDeletedRV = {};
      */
      coll.insertMany = jest.fn<typeof Collection.prototype.insertMany>();
      //.mockReturnValueOnce(Promise.resolve(insertManyRV));
      coll.applyPatches = jest.fn<typeof Collection.prototype.applyPatches>();
      //.mockReturnValueOnce(Promise.resolve(applyPatchesRV));
      coll.markAsDeleted = jest.fn<typeof Collection.prototype.markAsDeleted>();
      //.mockReturnValueOnce(Promise.resolve(markAsDeletedRV));

      const changeSet = {
        test: {
          insert: [],
          update: [],
          delete: [],
        },
      };

      /* const result = */ await dba.processChangeSet(changeSet);

      expect(coll.insertMany).toHaveBeenCalledWith(changeSet.test.insert);
      expect(coll.applyPatches).toHaveBeenCalledWith(changeSet.test.update);
      expect(coll.markAsDeleted).toHaveBeenCalledWith(changeSet.test.delete);
    });

    it("does not call wrong funcs", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      const coll = dba.collection("test");

      coll.insertMany = jest.fn<typeof Collection.prototype.insertMany>();
      coll.applyPatches = jest.fn<typeof Collection.prototype.applyPatches>();
      coll.markAsDeleted = jest.fn<typeof Collection.prototype.markAsDeleted>();

      await dba.processChangeSet({ test: {} });
      expect(coll.insertMany).not.toHaveBeenCalled();
      expect(coll.applyPatches).not.toHaveBeenCalled();
      expect(coll.markAsDeleted).not.toHaveBeenCalled();
    });
  }); /* procesChangeSet */

  describe("publishHelper", () => {
    it("appends updatedAt, returns toArray results in correct format", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      const coll = dba.collection("test");
      const cursor = coll.find({});

      const docs = [{ _id: new ObjectId() }, { _id: new ObjectId() }];
      cursor.toArray = jest
        .fn<typeof Cursor.prototype.toArray>()
        .mockReturnValueOnce(Promise.resolve(docs));

      const updatedAt = { test: 123 };
      const pubPropsStub = {
        updatedAt,
      } as unknown as PublicationProps<MongoDBA>;

      // results = { results: [ { coll: 'test', entries: [ [Object], [Object] ] } ] }
      const pubResult = await dba.publishHelper(cursor, pubPropsStub);

      expect(cursor.filter.__updatedAt).toEqual({ $gt: updatedAt.test });

      expect(typeof pubResult).toBe("object");
      const results = pubResult.results;

      expect(results).toBeDefined();
      expect(Array.isArray(results)).toBe(true);
      // @ts-expect-error: asserted above
      expect(results.length).toBe(1);

      // @ts-expect-error: asserted above
      const test = results[0];
      expect(test.coll).toBe("test");
      expect(test.entries.length).toBe(2);
      expect(test.entries[0]).toBe(docs[0]);
      expect(test.entries[1]).toBe(docs[1]);
    });

    it("works with empty query", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      const coll = dba.collection("test");
      const cursor = coll.find(); // <-- empty

      cursor.toArray = jest
        .fn<typeof Cursor.prototype.toArray>()
        .mockReturnValueOnce(Promise.resolve([]));

      const updatedAt = { test: 123 };
      const pubPropsStub = {
        updatedAt,
      } as unknown as PublicationProps<MongoDBA>;

      /* const results = */ await dba.publishHelper(cursor, pubPropsStub);
    });

    it("does not set updatedAt for irrelevant colls", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);
      const coll = dba.collection("test");
      const cursor = coll.find();

      cursor.toArray = jest
        .fn<typeof Cursor.prototype.toArray>()
        .mockReturnValueOnce(Promise.resolve([]));

      const pubPropsStub = {
        updatedAt: { notTest: 123 },
      } as unknown as PublicationProps<MongoDBA>;
      await dba.publishHelper(cursor, pubPropsStub);
      expect(cursor.filter.__updatedAt).not.toBeDefined();
    });

    it("returns same value for non-cursors", async () => {
      const dba = new MongoDBA(mongoUrl, "gongo", FakeMongoClient);

      const obj = {};
      const arr = [];
      const pubProps = {} as unknown as PublicationProps<MongoDBA>;

      expect(await dba.publishHelper(obj, pubProps)).toBe(obj);
      // if not a cursor, it should always be a doc.
      // expect(await dba.publishHelper(arr)).toBe(arr);
      // expect(await dba.publishHelper(1)).toBe(1);
      // expect(await dba.publishHelper("str")).toBe("str");
    });
  }); /* procesChangeSet */
});
