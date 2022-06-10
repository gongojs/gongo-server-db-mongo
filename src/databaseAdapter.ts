import { MongoClient as _MongoClient, ObjectId, Document } from "mongodb";
import type { Db } from "mongodb";
import type DatabaseAdapter from "gongo-server/lib/DatabaseAdapter.js";
import type {
  ChangeSet,
  ChangeSetUpdate,
  OpError,
} from "gongo-server/lib/DatabaseAdapter.js";
import type GongoServerless from "gongo-server/lib/serverless.js";
import type {
  PublicationProps,
  PublicationResult,
} from "gongo-server/lib/publications.js";

import Cursor from "./cursor";
import Collection from "./collection";
import Users from "./users";

class MongoDatabaseAdapter implements DatabaseAdapter<MongoDatabaseAdapter> {
  client: _MongoClient;
  dbPromise: Promise<Db>;
  collections: Record<string, Collection>;
  Users: Users;
  gs?: GongoServerless<MongoDatabaseAdapter>;

  constructor(url: string, dbName = "gongo", MongoClient = _MongoClient) {
    const client = (this.client = new MongoClient(url));

    this.dbPromise = new Promise((resolve, reject) => {
      client.connect((err) => {
        if (err) reject(err);
        resolve(client.db(dbName));
      });
    });

    this.collections = {};
    this.Users = new Users(this);
  }

  onInit(gs: GongoServerless<MongoDatabaseAdapter>) {
    const ARSON = gs.ARSON;
    this.gs = gs;

    // ARSON is actually called in gongo-server
    ARSON.registerType("ObjectID", {
      deconstruct: function (id: unknown) {
        //return id instanceof ObjectID && [ id.toHexString() ];

        if (id instanceof ObjectId) {
          // console.log(id, "instanceof ObjectID ==", id instanceof ObjectID);
          return [id.toHexString()];
        }

        if (typeof id === "object" && id !== null) {
          // Not quite sure in what circumstances this happens,
          // I guess multiple versions of ObjectID in use somehow.
          const keys = Object.keys(id); // [ '_bsontype', 'id' ]
          // console.log(id, keys, true);
          if (keys[0] === "_bsontype" && keys[1] === "id")
            return [(id as ObjectId).toHexString()];
        }

        return false;
      },

      reconstruct: function (args: Array<string>) {
        // https://github.com/benjamn/arson/blob/master/custom.js
        return args && ObjectId.createFromHexString(args[0]);
      },
    });
  }

  collection(name: string) {
    if (!this.collections[name])
      this.collections[name] = new Collection(this, name);

    return this.collections[name];
  }

  async insert(
    collName: string,
    entries: Array<Record<string, unknown>>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);

    try {
      const result = await coll.insertMany(entries as Document[]);
      if (!result.acknowledged) throw new Error("not acknolwedged");
      if (result.insertedCount !== entries.length)
        throw new Error("length mismatch");
    } catch (error) {
      // TODO, run them one by one.
      console.error("TODO skipping insertMany error", error);
    }
    return [];
  }

  async update(
    collName: string,
    updates: Array<ChangeSetUpdate>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);
    await coll.applyPatches(updates);
    return [];
  }

  async remove(collName: string, ids: Array<string>): Promise<Array<OpError>> {
    const coll = this.collection(collName);
    await coll.markAsDeleted(ids);
    return [];
  }

  async processChangeSet(changeSet: ChangeSet) {
    for (const [collName, ops] of Object.entries(changeSet)) {
      const coll = this.collection(collName);

      if (ops.insert) await coll.insertMany(ops.insert as Document[]);

      if (ops.update) {
        await coll.applyPatches(ops.update);
      }

      if (ops.delete) await coll.markAsDeleted(ops.delete);
    }

    // TODO, { $errors }
    return {};
  }

  async publishHelper(
    publishResult: Cursor | PublicationResult,
    { updatedAt }: PublicationProps<MongoDatabaseAdapter>
  ) {
    if (publishResult instanceof Cursor) {
      const collName = publishResult.coll.name;
      if (!publishResult.filter) publishResult.filter = {};
      if (updatedAt && updatedAt[collName])
        publishResult.filter.__updatedAt = { $gt: updatedAt[collName] };

      const helpedResult: PublicationResult = [];
      const entries = await publishResult.toArray();
      if (entries.length) helpedResult.push({ coll: collName, entries });
      return helpedResult;

      /*
      const out = [];
      const entries = await publishResult.toArray();
      if (entries.length) out.push({ coll: collName, entries });
      return out;
      */
    } else {
      return publishResult;
    }
  }
}

// At time of writing, Cursor / Collection was only used by Database class
// and exported just for testing.
export { ObjectId, Collection, Cursor };
export default MongoDatabaseAdapter;
