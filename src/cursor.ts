import type DatabaseAdapter from "./databaseAdapter.js";
import type Collection from "./collection.js";
import type { Document, Filter } from "mongodb";

export default class Cursor {
  db: DatabaseAdapter;
  coll: Collection;
  filter: Filter<Document>;

  constructor(coll: Collection, filter: Filter<Document>) {
    this.db = coll.db;
    this.coll = coll;
    this.filter = filter;
  }

  // TODO
  limit(limit: number) {
    throw new Error("limit not implemented yet");
  }

  async toArray() {
    const db = await this.db.dbPromise;
    const data = await db
      .collection(this.coll.name)
      .find(this.filter)
      .toArray();
    return data;
  }
}
