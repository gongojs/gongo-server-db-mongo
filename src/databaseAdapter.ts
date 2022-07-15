import {
  MongoClient as _MongoClient,
  ObjectId,
  Document,
  MongoBulkWriteError,
  WriteError,
} from "mongodb";
import type { Db } from "mongodb";
import type DatabaseAdapter from "gongo-server/lib/DatabaseAdapter.js";
import type {
  ChangeSetUpdate,
  OpError,
  DbaUser,
} from "gongo-server/lib/DatabaseAdapter.js";
import type GongoServerless from "gongo-server/lib/serverless.js";
import type {
  PublicationProps,
  PublicationResult,
} from "gongo-server/lib/publications.js";
import type { MethodProps } from "gongo-server";

import Cursor from "./cursor";
import Collection from "./collection";
import Users from "./users";
import type { CollectionEventProps } from "./collection";

export interface MongoDbaUser extends DbaUser {
  _id: ObjectId;
}

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
    entries: Array<Record<string, unknown>>,
    _props: MethodProps<MongoDatabaseAdapter>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);

    const preInsertManyProps: CollectionEventProps = {
      collection: coll,
      eventName: "preInsertMany",
      ..._props,
    };
    coll.eventExec("preInsertMany", preInsertManyProps, { entries });

    const toInsert: Document[] = [];
    const errors: OpError[] = [];
    for (const entry of entries) {
      if (Object.keys(entry).length === 1 && entry.$error) {
        // @ts-expect-error: TODO XXX
        errors.push([entry.$error.id, entry.$error.error]);
      } else {
        toInsert.push(entry);
      }
    }

    try {
      const result = await coll.insertMany(toInsert);
      if (!result.acknowledged) throw new Error("not acknolwedged");
      if (result.insertedCount !== entries.length)
        throw new Error("length mismatch");
    } catch (error) {
      if (error instanceof MongoBulkWriteError) {
        error.writeErrors;
        for (const writeError of error.writeErrors as Array<WriteError>) {
          // TODO, log full error on server?
          // TODO, "as string"... think more about types... objectid?  in code elsewhere
          // TODO, not tested yet!
          // TODO, should skip these on postInsertMany hook too!
          errors.push([
            toInsert[writeError.index]._id as string,
            writeError.errmsg,
          ]);
        }
      } else {
        // TODO, run them one by one.  XXX
        console.error("TODO skipping insertMany error", error);
      }
    }

    const postInsertManyProps: CollectionEventProps = {
      collection: coll,
      eventName: "postInsertMany",
      ..._props,
    }; // TODO skip non-inserted from mongo above
    coll.eventExec("postInsertMany", preInsertManyProps, { entries: toInsert });

    return errors;
  }

  async update(
    collName: string,
    updates: Array<ChangeSetUpdate>,
    _props: MethodProps<MongoDatabaseAdapter>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);
    const props: CollectionEventProps = {
      collection: coll,
      eventName: "update",
      ..._props,
    };
    await coll.applyPatches(updates);
    return [];
  }

  async remove(
    collName: string,
    ids: Array<string>,
    _props: MethodProps<MongoDatabaseAdapter>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);
    const props: CollectionEventProps = {
      collection: coll,
      eventName: "remove",
      ..._props,
    };

    await coll.markAsDeleted(ids);
    return [];
  }

  /*
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
  */

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
