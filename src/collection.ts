//const toMongoDb = require('jsonpatch-to-mongodb');
const toMongoDb = require("./jsonpatch-to-mongodb");
import Cursor from "./cursor.js";
import type DatabaseAdapter from "./databaseAdapter.js";
import type { ChangeSetUpdate } from "gongo-server/lib/DatabaseAdapter.js";
import type {
  Document,
  Filter,
  ReplaceOptions,
  UpdateFilter,
  UpdateOptions,
} from "mongodb";

export default class Collection {
  db: DatabaseAdapter;
  name: string;
  _indexCreated: boolean;
  allows: Record<string, unknown>;

  constructor(db: DatabaseAdapter, name: string) {
    this.db = db;
    this.name = name;
    this._indexCreated = false;
    this.allows = { insert: false, update: false, delete: false };
    // https://github.com/Meteor-Community-Packages/meteor-collection-hooks TODO
    // this.before = { insertOne: [] };
  }

  /*
  allow(operationName: string, func) {
    if (
      !Object.prototype.hasOwnProperty.call(
        this.allows.hasOwnProperty,
        operationName
      )
    )
      throw new Error(
        `No such operation "${operationName}", should be one of: ` +
          Object.keys(this.allows).join(", ")
      );

    if (this.allows[operationName])
      throw new Error(`Operation "${operationName}" is already set`);

    this.allows[operationName] = func;
  }
  */

  /*
  on(eventName, func) {
    if (this.events[eventName]) this.events[eventName].push(func);
    else throw new Error("No such event: " + eventName);
  }
  */

  /*
  eventExec(eventName, args) {
    if (!this.events[eventName]) throw new Error("No such event: " + eventName);

    for (let func of this.events[eventName]) func.call(this, args);
  }
  */

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

  find(filter: Filter<Document>) {
    // deal with __updatedAts
    // if NO __updatedAt specified, should NOT include deleted records
    //   (because we're getting data for first time!)

    return new Cursor(this, filter);
  }

  async findOne(filter: Filter<Document>) {
    const realColl = await this.getReal();
    return /* await */ realColl.findOne(filter);
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

    const bwArg = docArray.map((doc) => ({
      replaceOne: {
        filter: { _id: doc._id },
        // update: { $setOnInsert: doc },
        replacement: doc,
        upsert: true /* XXX TODO */,
      },
    }));

    await realColl.bulkWrite(bwArg);
  }

  async markAsDeleted(idArray: Array<string>) {
    const realColl = await this.getReal();
    console.log(this.name + " markAsDeleted: " + idArray.join(","));

    const now = Date.now();
    await realColl.bulkWrite(
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
    const update = toMongoDb(entry.patch) as UpdateFilter<Document>;

    /*
    updateOne does this already.
    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();
    */

    console.log("patch", entry, update);
    await this.updateOne({ _id }, update);
  }

  async applyPatches(entries: Array<ChangeSetUpdate>) {
    const realColl = await this.getReal();
    const bulk = [];

    for (const entry of entries) {
      console.log("patch", entry.patch);
      const update = toMongoDb(entry.patch);
      console.log("update", update);
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
    await realColl.bulkWrite(bulk);
  }
}
