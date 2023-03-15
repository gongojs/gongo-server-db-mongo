//const toMongoDb = require("jsonpatch-to-mongodb");
const toMongoDb = require("./jsonpatch-to-mongodb");
import Cursor from "./cursor";
import type DatabaseAdapter from "./databaseAdapter.js";
import type { ChangeSetUpdate } from "gongo-server/lib/DatabaseAdapter.js";
import type {
  Document,
  Filter,
  ReplaceOptions,
  UpdateFilter,
  UpdateOptions,
} from "mongodb";
import type { MethodProps } from "gongo-server";
import type { OpError } from "gongo-server/lib/DatabaseAdapter.js";
import { ObjectId } from "mongodb";

// https://advancedweb.hu/how-to-use-async-functions-with-array-filter-in-javascript/
// I added types.
/*
const asyncFilter = async <T>(
  arr: T[],
  predicate: (item: T) => Promise<boolean>
) =>
  Promise.all(arr.map(predicate)).then((results) =>
    arr.filter((_v, index) => results[index])
  );
*/

// https://stackoverflow.com/a/51399781/1839099
type ArrayElement<ArrayType extends readonly unknown[]> =
  ArrayType extends readonly (infer ElementType)[] ? ElementType : never;

export interface CollectionEventProps extends MethodProps<DatabaseAdapter> {
  collection: Collection;
  eventName: string;
}

export type AllowInsertHandler = (
  doc: Document,
  props: CollectionEventProps
) => Promise<boolean | string>;
export type AllowUpdateHandler = (
  update: ChangeSetUpdate,
  props: CollectionEventProps
) => Promise<boolean | string>;
export type AllowRemoveHandler = (
  id: string,
  props: CollectionEventProps
) => Promise<boolean | string>;
export type AllowHandler =
  | AllowInsertHandler
  | AllowUpdateHandler
  | AllowRemoveHandler;

type EventFunction = (
  props: CollectionEventProps,
  args?: Record<string, unknown>
) => void;

type OperationName = "insert" | "update" | "remove";

interface Allows {
  insert: false | AllowInsertHandler;
  update: false | AllowUpdateHandler;
  remove: false | AllowRemoveHandler;
}

type EventName = "preInsertMany" | "postInsertMany" | "postUpdateMany";

export async function userIsAdmin(
  _doc: Document | ChangeSetUpdate | string,
  { dba, auth }: CollectionEventProps
) {
  const userId = await auth.userId();
  if (!userId) return "NOT_LOGGED_IN";

  const user = await dba.collection("users").findOne({ _id: userId });
  if (!user || !user.admin) return "NOT_ADMIN";

  return true;
}

export async function userIdMatches(
  doc: Document | ChangeSetUpdate | string,
  { dba, auth, collection, eventName }: CollectionEventProps
) {
  const userId = await auth.userId();
  if (!userId) return "NOT_LOGGED_IN";

  if (typeof doc === "object" && "patch" in doc) {
    // Update
    const docId = typeof doc._id === "string" ? new ObjectId(doc._id) : doc._id;
    const existingDoc = await collection.findOne(docId);
    if (!existingDoc) return "NO_EXISTING_DOC";
    return userId.equals(existingDoc.userId) || "doc.userId !== userId";
  }

  // TODO, for delete

  console.log({ doc, userId });
  // @ts-expect-error: TODO
  return userId.equals(doc.userId) || "doc.userId !== userId";
}

export default class Collection {
  db: DatabaseAdapter;
  name: string;
  _indexCreated: boolean;
  _allows: Allows;
  _events: Record<EventName, Array<EventFunction>>;

  constructor(db: DatabaseAdapter, name: string) {
    this.db = db;
    this.name = name;
    this._indexCreated = false;
    this._allows = { insert: false, update: false, remove: false };
    // https://github.com/Meteor-Community-Packages/meteor-collection-hooks TODO
    // this.before = { insertOne: [] };
    this._events = {
      preInsertMany: [],
      postInsertMany: [],
      postUpdateMany: [],
    };
  }

  allow(operationName: "insert", func: AllowInsertHandler): void;
  allow(operationName: "update", func: AllowUpdateHandler): void;
  allow(operationName: "remove", func: AllowRemoveHandler): void;
  allow(operationName: OperationName, func: AllowHandler) {
    if (
      operationName !== "insert" &&
      operationName !== "update" &&
      operationName !== "remove"
    )
      throw new Error(
        `No such operation "${operationName}", should be one of: ` +
          Object.keys(this._allows).join(", ")
      );

    if (this._allows[operationName])
      throw new Error(`Operation "${operationName}" is already set`);

    if (operationName === "insert")
      this._allows.insert = func as AllowInsertHandler;
    else if (operationName === "update")
      this._allows.update = func as AllowUpdateHandler;
    else if (operationName === "remove")
      this._allows.remove = func as AllowRemoveHandler;

    // this._allows[operationName] = func;
  }

  /**
   * Called by gongo-server/crud to go through all requested changes
   * and return valid docs for insert/update/remove.
   * @param docs An array of docs
   */
  async allowFilter(
    operationName: "insert",
    docs: Array<Document>,
    props: CollectionEventProps,
    errors: Array<OpError>
  ): Promise<Array<Document>>;
  async allowFilter(
    operationName: "update",
    docs: Array<ChangeSetUpdate>,
    props: CollectionEventProps,
    errors: Array<OpError>
  ): Promise<Array<Record<string, unknown>>>;
  async allowFilter(
    operationName: "remove",
    docs: Array<string>,
    props: CollectionEventProps,
    errors: Array<OpError>
  ): Promise<Array<string>>;
  async allowFilter(
    operationName: OperationName,
    docs: Array<Document | ChangeSetUpdate | string>,
    props: CollectionEventProps,
    errors: Array<OpError>
  ) {
    const allowHandler = this._allows[operationName];

    if (!allowHandler) {
      errors.push(
        ...docs.map((doc) => {
          // @ts-expect-error: i hate you typescript
          const id = operationName === "remove" ? doc : doc._id;
          return [id, `No "${operationName}" allow handler`] as OpError;
        })
      );
      return [];
      /*
      throw new Error(
        `Collection "${this.name}" has no allow handler for operation "${operationName}"`
      );
      */
    }

    if (operationName === "insert") {
      const filtered = [];
      const insertAllowHandler = allowHandler as AllowInsertHandler;
      for (const doc of docs) {
        const result = await insertAllowHandler(doc as Document, props);
        if (result === true) filtered.push(doc);
        else errors.push([(doc as Document)._id, result]);
      }
      return filtered;
    } else if (operationName === "update") {
      const filtered = [];
      const updateAllowHandler = allowHandler as AllowUpdateHandler;
      for (const doc of docs) {
        const result = await updateAllowHandler(doc as ChangeSetUpdate, props);
        if (result === true) filtered.push(doc);
        else errors.push([(doc as ChangeSetUpdate)._id, result]);
      }
      return filtered;
    } else if (operationName === "remove") {
      const filtered = [];
      const removeAllowHandler = allowHandler as AllowRemoveHandler;
      for (const id of docs) {
        const result = await removeAllowHandler(id as string, props);
        if (result === true) filtered.push(id);
        else errors.push([id as string, result]);
      }
      return filtered;
    } else {
      throw new Error(
        `Invalid operation name "${operationName}", ` +
          'expected: "insert" | "update" | "remove".'
      );
    }
  }

  on(eventName: EventName, func: EventFunction) {
    if (this._events[eventName]) this._events[eventName].push(func);
    else throw new Error("No such event: " + eventName);
  }

  eventExec(
    eventName: EventName,
    props: CollectionEventProps,
    args?: Record<string, unknown>
  ) {
    if (!this._events[eventName])
      throw new Error("No such event: " + eventName);

    for (const func of this._events[eventName]) func.call(this, props, args);
  }

  async getReal() {
    const db = await this.db.dbPromise;
    const realColl = db.collection(this.name);

    // TODO is serverless the best place for this?
    if (!this._indexCreated) {
      this._indexCreated = true;

      // don't await.
      realColl.createIndex("__updatedAt");
    }

    return realColl;
  }

  find(filter: Filter<Document> = {}) {
    // deal with __updatedAts
    // if NO __updatedAt specified, should NOT include deleted records
    //   (because we're getting data for first time!)

    return new Cursor(this, filter);
  }

  async findOne(filter: Filter<Document>) {
    const realColl = await this.getReal();
    return await realColl.findOne(filter);
  }

  async insertOne(doc: Document) {
    const realColl = await this.getReal();

    doc.__updatedAt = Date.now();
    console.log(this.name + " insert: " + JSON.stringify(doc));
    return await realColl.insertOne(doc);
  }

  /*
  insert existing id should fail silently
  delete a non-existing id should fail silently
  Fow now.  think if these errors should go back to client.
  In bigger scheme of things, all we care is that client
  gets correct server copy
   */

  async insertMany(docArray: Array<Document>) {
    const realColl = await this.getReal();

    const now = Date.now();
    for (const doc of docArray) doc.__updatedAt = now;

    console.log(this.name + " insertMany: " + JSON.stringify(docArray));
    //return await realColl.insertMany(docArray, { ordered: false /* XXX TODO */ });

    /*
    const bwArg = docArray.map((doc) => ({
      replaceOne: {
        filter: { _id: doc._id },
        // update: { $setOnInsert: doc },
        replacement: doc,
        upsert: true /* XXX TODO */ /*,
      },
    }));

    return await realColl.bulkWrite(bwArg);
    */

    return await realColl.insertMany(docArray);
  }

  async markAsDeleted(idArray: Array<string>) {
    const realColl = await this.getReal();
    console.log(this.name + " markAsDeleted: " + idArray.join(","));

    const now = Date.now();
    return await realColl.bulkWrite(
      /// XXX XXX XXX does this even work?  need ObjectId(id)
      idArray.map((id) => ({
        replaceOne: {
          filter: { _id: id },
          replacement: { _id: id, __deleted: true, __updatedAt: now },
          upsert: true /* XXX TODO */,
        },
      }))
    );
  }

  async replaceOne(
    filter: Filter<Document>,
    doc: Document,
    options?: ReplaceOptions
  ) {
    const realColl = await this.getReal();

    if (!doc) throw new Error("not replacing " + filter + " with empty doc");

    doc.__updatedAt = Date.now();

    if (options) return realColl.replaceOne(filter, doc, options);
    else return realColl.replaceOne(filter, doc);
  }

  async updateOne(
    filter: Filter<Document>,
    update: Partial<Document> | UpdateFilter<Document> = {},
    options?: UpdateOptions
  ) {
    const realColl = await this.getReal();

    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();

    if (options) return realColl.updateOne(filter, update, options);
    else return realColl.updateOne(filter, update);
  }

  async applyPatch(entry: ChangeSetUpdate) {
    const _id = entry._id;
    const orig = await this.findOne({ _id });
    const update = toMongoDb(entry.patch, orig) as UpdateFilter<Document>;

    /*
    updateOne does this already.
    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();
    */

    console.log("patch", entry, update);
    return await this.updateOne({ _id }, update);
  }

  async applyPatches(entries: Array<ChangeSetUpdate>) {
    const realColl = await this.getReal();

    const ids = entries.map((doc) => doc._id);
    const origResult = await this.find({ _id: { $in: ids } }).toArray();
    const origDocs: Record<string, ArrayElement<typeof origResult>> = {};
    for (const doc of origResult) {
      origDocs[doc._id.toString()] = doc;
    }

    const bulk = [];

    //for (const entry of entries) {
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      const orig = origDocs[entry._id.toString()];
      console.log("patch", entry.patch);
      const update = toMongoDb(entry.patch, orig);
      console.log("update", update, orig);
      if (!update.$set) update.$set = {};
      update.$set.__updatedAt = Date.now();

      bulk.push({
        updateOne: {
          filter: { _id: entry._id },
          update,
        },
      });
    }

    console.log(JSON.stringify(bulk, null, 2));
    return await realColl.bulkWrite(bulk);
  }
}
