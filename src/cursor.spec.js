const Collection = require("./collection");
const Cursor = require("./cursor");

describe("Cursor", () => {
  describe("constructor", () => {
    it("stores instance vars", () => {
      const db = {};
      const coll = new Collection(db, "collection");
      const cursor = new Cursor(coll, "query");

      expect(cursor.db).toBe(db);
      expect(cursor.coll).toBe(coll);
      expect(cursor.query).toBe("query");
    });
  });

  describe("toArray", () => {
    it("runs query on real collection", async () => {
      const mongoDb = { collection: jest.fn() };
      const mongoCol = { find: jest.fn(), name: "collection" };
      const mongoQuery = { toArray: jest.fn() };
      const mongoResults = [];

      mongoDb.collection.mockReturnValueOnce(mongoCol);
      mongoCol.find.mockReturnValueOnce(mongoQuery);
      mongoQuery.toArray.mockReturnValueOnce(Promise.resolve(mongoResults));

      const db = { dbPromise: Promise.resolve(mongoDb) };
      const coll = new Collection(db, "collection");
      const cursor = new Cursor(coll, "query");

      const results = await cursor.toArray();
      expect(mongoDb.collection).toHaveBeenCalledWith("collection");
      expect(mongoCol.find).toHaveBeenCalledWith("query");
      expect(results).toBe(mongoResults);
    });
  });
});
