import Collection from "./collection";
import Cursor from "./cursor";

describe("Cursor", () => {
  describe("constructor", () => {
    it("stores instance vars", () => {
      const db = {};
      // @ts-expect-error: stub
      const coll = new Collection(db, "collection");
      // @ts-expect-error: stub
      const cursor = new Cursor(coll, "filter");

      expect(cursor.db).toBe(db);
      expect(cursor.coll).toBe(coll);
      expect(cursor.filter).toBe("filter");
    });
  });

  describe("toArray", () => {
    it("runs query on real collection", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { find: jest.fn(), name: "collection" };
      const mongoQuery = { toArray: jest.fn() };
      const mongoResults: Array<Record<string, unknown>> = [];

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.find.mockReturnValueOnce(mongoQuery);
      mongoQuery.toArray.mockReturnValueOnce(Promise.resolve(mongoResults));

      const db = { dbPromise: Promise.resolve(mongoDb) };
      // @ts-expect-error: stub
      const coll = new Collection(db, "collection");
      // @ts-expect-error: stub
      const cursor = new Cursor(coll, "query");

      const results = await cursor.toArray();
      expect(mongoDb.collection).toHaveBeenCalledWith("collection");
      expect(mongoCol.find).toHaveBeenCalledWith("query");
      expect(results).toBe(mongoResults);
    });
  });
});
