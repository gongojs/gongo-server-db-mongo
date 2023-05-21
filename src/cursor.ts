import type DatabaseAdapter from "./databaseAdapter.js";
import type Collection from "./collection.js";
import type { GongoDocument } from "./collection.js";
import type {
  Filter,
  Sort,
  SortDirection,
  Document as MongoDocument,
} from "mongodb";

export default class Cursor<DocType extends GongoDocument = GongoDocument> {
  db: DatabaseAdapter;
  coll: Collection<DocType>;
  filter: Filter<DocType>;
  _limit: number | null = null;
  _sort: { sort: Sort; direction?: SortDirection } | null = null;
  _skip: number | null = null;
  _project: MongoDocument | null = null;

  constructor(coll: Collection<DocType>, filter: Filter<DocType>) {
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

  project<T extends MongoDocument>(value: MongoDocument) {
    this._project = value;
    return this as unknown as Cursor<T>;
  }

  async toArray() {
    const db = await this.db.dbPromise;

    const cursor = db.collection<DocType>(this.coll.name).find(this.filter);

    if (this._sort) cursor.sort(this._sort.sort, this._sort.direction);
    if (this._limit) cursor.limit(this._limit);
    if (this._skip) cursor.skip(this._skip);
    if (this._project) cursor.project(this._project);

    const data = await cursor.toArray();
    return data;
  }
}
