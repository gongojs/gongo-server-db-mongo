import type DatabaseAdapter from "./databaseAdapter.js";
import type Collection from "./collection.js";
import type { Document, Filter, Sort, SortDirection } from "mongodb";

export default class Cursor {
  db: DatabaseAdapter;
  coll: Collection;
  filter: Filter<Document>;
  _limit: number | null = null;
  _sort: { sort: Sort; direction?: SortDirection } | null = null;
  _skip: number | null = null;

  constructor(coll: Collection, filter: Filter<Document>) {
    this.db = coll.db;
    this.coll = coll;
    this.filter = filter;
  }

  sort(sort: Sort | string, direction?: SortDirection): this {
    this._sort = { sort, direction };
    return this;
  }

  limit(limit: number): this {
    this._limit = limit;
    return this;
  }

  skip(skip: number): this {
    this._skip = skip;
    return this;
  }

  async toArray() {
    const db = await this.db.dbPromise;

    const cursor = db.collection(this.coll.name).find(this.filter);

    if (this._sort) cursor.sort(this._sort.sort, this._sort.direction);
    if (this._limit) cursor.limit(this._limit);
    if (this._skip) cursor.skip(this._skip);

    const data = await cursor.toArray();
    return data;
  }
}
