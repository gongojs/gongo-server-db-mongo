//const toMongoDb = require("jsonpatch-to-mongodb");
const toMongoDb = require("./jsonpatch-to-mongodb");
import Cursor from "./cursor";
import type DatabaseAdapter from "./databaseAdapter.js";
import type { ChangeSetUpdate } from "gongo-server/lib/DatabaseAdapter.js";
import type {
  Document as MongoDocument,
  Filter,
  ReplaceOptions,
  UpdateFilter,
  UpdateOptions,
  OptionalUnlessRequiredId,
} from "mongodb";
import type { MethodProps } from "gongo-server";
import type { OpError } from "gongo-server/lib/DatabaseAdapter.js";
import { ObjectId } from "./objectid";
import * as jsonpatch from "fast-json-patch";

// https://github.com/mongodb/node-mongodb-native/blob/b67af3cd/src/mongo_types.ts#L46 thanks Mongo team
/** TypeScript Omit (Exclude to be specific) does not work for objects with an "any" indexed type, and breaks discriminated unions @public */
export type EnhancedOmit<TRecordOrUnion, KeyUnion> =
  string extends keyof TRecordOrUnion
    ? TRecordOrUnion // TRecordOrUnion has indexed type e.g. { _id: string; [k: string]: any; } or it is "any"
    : // eslint-disable-next-line @typescript-eslint/no-explicit-any
    TRecordOrUnion extends any
    ? Pick<TRecordOrUnion, Exclude<keyof TRecordOrUnion, KeyUnion>> // discriminated unions
    : never;

export interface GongoDocument extends MongoDocument {
  __deleted?: boolean;
  __updatedAt?: number;
}

export interface ChangeSetUpdateResult {
  _id: string;
  $success?: boolean;
  $error?: string;
}

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

export interface CollectionEventProps<
  DocType extends GongoDocument = GongoDocument,
> extends MethodProps<DatabaseAdapter> {
  collection: Collection<DocType>;
  eventName: string;
}

export type AllowInsertHandler<DocType extends GongoDocument> = (
  doc: DocType,
  props: CollectionEventProps<DocType>,
) => Promise<boolean | string>;
export type AllowUpdateHandler<DocType extends GongoDocument> = (
  update: ChangeSetUpdate,
  props: CollectionEventProps<DocType>,
) => Promise<boolean | string>;
export type AllowRemoveHandler<DocType extends GongoDocument> = (
  id: string,
  props: CollectionEventProps<DocType>,
) => Promise<boolean | string>;
export type AllowHandler<DocType extends GongoDocument> =
  | AllowInsertHandler<DocType>
  | AllowUpdateHandler<DocType>
  | AllowRemoveHandler<DocType>;

type EventFunction<DocType extends GongoDocument> = (
  props: CollectionEventProps<DocType>,
  args?: Record<string, unknown>,
) => void;

type OperationName = "insert" | "update" | "remove";

interface Allows<DocType extends GongoDocument> {
  insert: false | AllowInsertHandler<DocType>;
  update: false | AllowUpdateHandler<DocType>;
  remove: false | AllowRemoveHandler<DocType>;
}

type EventName = "preInsertMany" | "postInsertMany" | "postUpdateMany";

export async function userIsAdmin<DocType extends GongoDocument>(
  _doc: DocType | ChangeSetUpdate | string,
  { dba, auth }: CollectionEventProps<DocType>,
) {
  const userId = await auth.userId();
  if (!userId) return "NOT_LOGGED_IN";

  const user = await dba.collection("users").findOne({ _id: userId });
  if (!user || !user.admin) return "NOT_ADMIN";

  return true;
}

export async function userIdMatches<DocType extends GongoDocument>(
  doc: DocType | ChangeSetUpdate | string,
  { auth, collection, eventName }: CollectionEventProps<DocType>,
) {
  const userId = await auth.userId();
  if (!userId) return "NOT_LOGGED_IN";

  // TODO, use this instead of inspecting doc? :)
  eventName;

  // UPDATES (doc is a ChangeSetUpdate)
  if (typeof doc === "object" && "patch" in doc) {
    const docId = typeof doc._id === "string" ? new ObjectId(doc._id) : doc._id;
    const existingDoc = await collection.findOne(docId);
    if (!existingDoc) return "NO_EXISTING_DOC";
    return (
      userId.equals(existingDoc.userId) || "doc.userId !== userId (for patch)"
    );
  }

  // DELETES (doc is an ObjectId)
  if (doc instanceof ObjectId || typeof doc === "string") {
    const query: Partial<GongoDocument> = {
      _id: typeof doc === "string" ? new ObjectId(doc) : doc,
    };
    const existingDoc = await collection.findOne(query);
    if (!existingDoc) return "NO_EXISTING_DOC";
    return (
      userId.equals(existingDoc.userId) || "doc.userId !== userId (for delete)"
    );
  }

  /*
  console.log({ doc, userId });
  console.log(1, userId.toHexString());
  console.log(2, doc.userId?.toString());
  console.log(3, userId.equals(doc.userId));
  */

  return (
    userId.toHexString() === (doc.userId?.toHexString() || doc.userId) ||
    "doc.userId !== userId (for unmatched)"
  );
  // [TypeError: Cannot read properties of undefined (reading '11')]
  // return userId.equals(doc.userId) || "doc.userId !== userId (for unmatched)";
}

export default class Collection<DocType extends GongoDocument = GongoDocument> {
  db: DatabaseAdapter;
  name: string;
  _indexCreated: boolean;
  _allows: Allows<DocType>;
  _events: Record<EventName, Array<EventFunction<DocType>>>;

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

  allow(operationName: "insert", func: AllowInsertHandler<DocType>): void;
  allow(operationName: "update", func: AllowUpdateHandler<DocType>): void;
  allow(operationName: "remove", func: AllowRemoveHandler<DocType>): void;
  allow(operationName: OperationName, func: AllowHandler<DocType>) {
    if (
      operationName !== "insert" &&
      operationName !== "update" &&
      operationName !== "remove"
    )
      throw new Error(
        `No such operation "${operationName}", should be one of: ` +
          Object.keys(this._allows).join(", "),
      );

    if (this._allows[operationName])
      throw new Error(`Operation "${operationName}" is already set`);

    if (operationName === "insert")
      this._allows.insert = func as AllowInsertHandler<DocType>;
    else if (operationName === "update")
      this._allows.update = func as AllowUpdateHandler<DocType>;
    else if (operationName === "remove")
      this._allows.remove = func as AllowRemoveHandler<DocType>;

    // this._allows[operationName] = func;
  }

  /**
   * Called by gongo-server/crud to go through all requested changes
   * and return valid docs for insert/update/remove.
   * @param docs An array of docs
   */
  async allowFilter(
    operationName: "insert",
    docs: Array<DocType>,
    props: CollectionEventProps<DocType>,
    errors: Array<OpError>,
  ): Promise<Array<DocType>>;
  async allowFilter(
    operationName: "update",
    docs: Array<ChangeSetUpdate>,
    props: CollectionEventProps<DocType>,
    errors: Array<OpError>,
  ): Promise<Array<DocType>>;
  async allowFilter(
    operationName: "remove",
    docs: Array<string>,
    props: CollectionEventProps<DocType>,
    errors: Array<OpError>,
  ): Promise<Array<string>>;
  async allowFilter(
    operationName: OperationName,
    docs: Array<DocType | ChangeSetUpdate | string>,
    props: CollectionEventProps<DocType>,
    errors: Array<OpError>,
  ) {
    const allowHandler = this._allows[operationName];

    if (!allowHandler) {
      errors.push(
        ...docs.map((doc) => {
          // @ts-expect-error: i hate you typescript
          const id = operationName === "remove" ? doc : doc._id;
          return [id, `No "${operationName}" allow handler`] as OpError;
        }),
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
      const insertAllowHandler = allowHandler as AllowInsertHandler<DocType>;
      for (const doc of docs) {
        const result = await insertAllowHandler(doc as DocType, props);
        if (result === true) filtered.push(doc);
        else errors.push([(doc as DocType)._id, result]);
      }
      return filtered;
    } else if (operationName === "update") {
      const filtered = [];
      const updateAllowHandler = allowHandler as AllowUpdateHandler<DocType>;
      for (const doc of docs) {
        const result = await updateAllowHandler(doc as ChangeSetUpdate, props);
        if (result === true) filtered.push(doc);
        else errors.push([(doc as ChangeSetUpdate)._id, result]);
      }
      return filtered;
    } else if (operationName === "remove") {
      const filtered = [];
      const removeAllowHandler = allowHandler as AllowRemoveHandler<DocType>;
      for (const id of docs) {
        const result = await removeAllowHandler(id as string, props);
        if (result === true) filtered.push(id);
        else errors.push([id as string, result]);
      }
      return filtered;
    } else {
      throw new Error(
        `Invalid operation name "${operationName}", ` +
          'expected: "insert" | "update" | "remove".',
      );
    }
  }

  on(eventName: EventName, func: EventFunction<DocType>) {
    if (this._events[eventName]) this._events[eventName].push(func);
    else throw new Error("No such event: " + eventName);
  }

  async eventExec(
    eventName: EventName,
    props: CollectionEventProps<DocType>,
    args?: Record<string, unknown>,
  ) {
    if (!this._events[eventName])
      throw new Error("No such event: " + eventName);

    for (const func of this._events[eventName])
      await func.call(this, props, args);
  }

  async getReal() {
    const db = await this.db.dbPromise;
    const realColl = db.collection<DocType>(this.name);

    // TODO is serverless the best place for this?
    if (!this._indexCreated) {
      this._indexCreated = true;

      // don't await.
      realColl.createIndex("__updatedAt");
    }

    return realColl;
  }

  find(filter: Filter<DocType> = {}) {
    // deal with __updatedAts
    // if NO __updatedAt specified, should NOT include deleted records
    //   (because we're getting data for first time!)

    return new Cursor(this, filter);
  }

  async findOne(filter: Filter<DocType>) {
    const realColl = await this.getReal();
    return await realColl.findOne(filter);
  }

  async insertOne(doc: OptionalUnlessRequiredId<DocType>) {
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

  async insertMany(docArray: OptionalUnlessRequiredId<DocType>[]) {
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
      /// XXX  fixed with typescript work.
      idArray.map((id) => ({
        replaceOne: {
          filter: { _id: new ObjectId(id) } as Partial<GongoDocument>,
          replacement: {
            __deleted: true,
            __updatedAt: now,
          } as GongoDocument as DocType,
          upsert: true /* XXX TODO */,
        },
      })),
    );
  }

  async replaceOne(
    filter: Filter<DocType>,
    doc: DocType,
    options?: ReplaceOptions,
  ) {
    const realColl = await this.getReal();

    if (!doc) throw new Error("not replacing " + filter + " with empty doc");

    doc.__updatedAt = Date.now();

    if (options) return realColl.replaceOne(filter, doc, options);
    else return realColl.replaceOne(filter, doc);
  }

  async updateOne(
    filter: Filter<DocType>,
    update: Partial<DocType> | UpdateFilter<DocType> = {},
    options?: UpdateOptions,
  ) {
    const realColl = await this.getReal();

    /*
    const test1: GongoDocument = { __updatedAt: 1 };
    const test2: DocType = { __updatedAt: 1 };
    if (update.$set) update.$set.__updatedAt = Date.now();
    else update.$set = { __updatedAt: Date.now() };
    */

    if (update.$set) {
      update.$set = {
        ...update.$set,
        __updatedAt: Date.now(),
      };
    } else {
      // @ts-expect-error: TODO, another day
      update.$set = {
        __updatedAt: Date.now(),
      };
    }

    if (options) return realColl.updateOne(filter, update, options);
    else return realColl.updateOne(filter, update);
  }

  async applyPatch(entry: ChangeSetUpdate) {
    // XXX was string before update.
    const idFilter = { _id: new ObjectId(entry._id) } as Partial<GongoDocument>;
    const orig = await this.findOne(idFilter);
    let fallback = false;

    const update = (function () {
      try {
        return toMongoDb(entry.patch, orig) as UpdateFilter<DocType>;
      } catch (error) {
        console.log("Skipping toMongoDb update because of error.");
        console.log(error);
        fallback = true;
        return null;
      }
    })();

    /*
    updateOne does this already.
    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();
    */

    if (update) {
      console.log("patch", entry, update);

      const result = await (async () => {
        try {
          return await this.updateOne(idFilter, update);
        } catch (error) {
          console.log("Skipping updateOne update because of error.");
          console.log(error);
          fallback = true;
          return null;
        }
      })();

      if (result) {
        console.log(result);
        return result;
      }
    }

    if (fallback) {
      console.log("Falling back to apply patch directly and replaceOne");
      const result = { ...orig };
      const validateOperation = true;

      for (const operation of entry.patch) {
        // https://github.com/Starcounter-Jack/JSON-Patch/issues/280#issuecomment-1980435509
        try {
          jsonpatch.applyOperation(result, operation, validateOperation);
        } catch (e) {
          const error = e as jsonpatch.JsonPatchError;
          // Try to recover:
          if (error.name === "OPERATION_PATH_UNRESOLVABLE") {
            if (operation.op === "replace") {
              // Can happen e.g. when states are like this:
              // from.schaden = undefined;
              // to.schaden.id = 'some-id';
              // @ts-expect-error: but its exactly what i want to do.
              operation.op = "add"; // try it once more with operation "add" instead
              jsonpatch.applyOperation(result, operation, validateOperation);
            } else if (operation.op === "remove") {
              // Can happen e.g. when states are like this:
              // from.entity.begruendung = null;
              // to.entity.begruendung = undefined;
              // we don't do anything in this case because "to" is already in a good state!
            }
          } else {
            // otherwise we just rethrow ...
            throw error;
          }
        }
      }

      // If we got this far, there were no errors.
      // @ts-expect-error: another day
      this.replaceOne(idFilter, result);

      /*
      const result = jsonpatch.applyPatch(orig, entry.patch, true);
      console.log(result);

      if (result.newDocument) {
        // @ts-expect-error: another day
        this.replaceOne(idFilter, result.newDocument);
      } else console.log(result);
      throw new Error("No newDocument on jsonpatch.applyPatch result");
      */
    }

    return { _id: entry._id, $success: true };
  }

  async applyPatches(entries: Array<ChangeSetUpdate>) {
    return await Promise.all(entries.map(this.applyPatch.bind(this)));
  }

  async applyPatches1(entries: Array<ChangeSetUpdate>) {
    const realColl = await this.getReal();

    const ids = entries.map((doc) => doc._id);
    // @ts-expect-error: save for another day
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
          // XXX was string before last update
          filter: { _id: new ObjectId(entry._id) } as Partial<GongoDocument>,
          update,
        },
      });
    }

    console.log(JSON.stringify(bulk, null, 2));
    return await realColl.bulkWrite(bulk);
  }
}
