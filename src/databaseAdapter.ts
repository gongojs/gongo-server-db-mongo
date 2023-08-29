/*
import {
  MongoClient as _MongoClient,
  MongoBulkWriteError,
  WriteError,
} from "mongodb";
*/
import type {
  MongoBulkWriteError as MongoBulkWriteErrorType,
  WriteError as WriteErrorType,
} from "mongodb";

// @ts-expect-error: yup
import * as bulkCommon from "mongodb/lib/bulk/common";

const {
  MongoBulkWriteError,
}: {
  MongoBulkWriteError: typeof MongoBulkWriteErrorType;
} = bulkCommon;

import { ObjectId } from "bson";
import type { MongoClient as _MongoClient, Db, Document } from "mongodb";
import type DatabaseAdapter from "gongo-server/lib/DatabaseAdapter.js";
import type {
  ChangeSetUpdate,
  OpError,
  DbaUser,
  // ChangeSet,
} from "gongo-server/lib/DatabaseAdapter.js";
import type GongoServerless from "gongo-server/lib/serverless.js";
import type {
  PublicationProps,
  PublicationResult,
} from "gongo-server/lib/publications.js";
import type { MethodProps } from "gongo-server";

import Cursor from "./cursor";
import Collection, { GongoDocument } from "./collection";
import Users from "./users";
import type { CollectionEventProps } from "./collection";

export interface MongoDbaUser extends DbaUser {
  _id: ObjectId;
}

class MongoDatabaseAdapter implements DatabaseAdapter<MongoDatabaseAdapter> {
  client: _MongoClient;
  dbPromise: Promise<Db>;
  collections: Record<string, Collection<GongoDocument>>;
  Users: Users;
  gs?: GongoServerless<MongoDatabaseAdapter>;

  constructor(
    urlOrMongoClientInstance: string | _MongoClient,
    dbName = "gongo",
    MongoClient?: typeof _MongoClient
  ) {
    const client = (this.client = (function () {
      if (typeof urlOrMongoClientInstance === "string") {
        if (!MongoClient)
          throw new Error("URL provided but MongoClient not provided");
        return new MongoClient(urlOrMongoClientInstance);
      } else {
        return urlOrMongoClientInstance;
      }
    })());

    this.dbPromise = client.connect().then((client) => client.db(dbName));

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

          if (
            "toExtendedJSON" in id &&
            typeof id.toExtendedJSON === "function"
          ) {
            const result = id.toExtendedJSON();
            if (typeof result === "object" && result["$oid"]) {
              return [result["$oid"]];
            }
          }

          if ("inspect" in id && typeof id.inspect === "function") {
            const result = id.inspect();
            if (typeof result === "string") {
              const match = result.match(
                /new ObjectId\("(?<hexId>[0-9a-f]{24})"\)/
              );
              if (match && match.groups) return [match.groups.hexId];
            }
          }
        }

        return false;
      },

      reconstruct: function (args: Array<string>) {
        // https://github.com/benjamn/arson/blob/master/custom.js
        return args && ObjectId.createFromHexString(args[0]);
      },
    });
  }

  collection<DocType extends GongoDocument>(name: string): Collection<DocType> {
    if (!this.collections[name])
      // @ts-expect-error: for another day
      this.collections[name] = new Collection<DocType>(this, name);

    // @ts-expect-error: for another day
    return this.collections[name] as Collection<DocType>;
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
        for (const writeError of error.writeErrors as Array<WriteErrorType>) {
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
    coll.eventExec("postInsertMany", postInsertManyProps, {
      entries: toInsert,
    });

    return errors;
  }

  async update(
    collName: string,
    updates: Array<ChangeSetUpdate>,
    _props: MethodProps<MongoDatabaseAdapter>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);

    /*
    const props: CollectionEventProps = {
      collection: coll,
      eventName: "update",
      ..._props,
    };
    */

    await coll.applyPatches(updates);

    const postUpdateManyProps: CollectionEventProps = {
      collection: coll,
      eventName: "postUpdateMany",
      ..._props,
    };
    coll.eventExec("postUpdateMany", postUpdateManyProps, { entries: updates });

    return [];
  }

  async remove(
    collName: string,
    ids: Array<string>
    // _props: MethodProps<MongoDatabaseAdapter>
  ): Promise<Array<OpError>> {
    const coll = this.collection(collName);

    /*
    const props: CollectionEventProps = {
      collection: coll,
      eventName: "remove",
      ..._props,
    };
    */

    await coll.markAsDeleted(ids);
    return [];
  }

  async allowFilter(
    collName: string,
    operationName: "insert",
    docs: Array<Record<string, unknown>>,
    _props: MethodProps<MongoDatabaseAdapter>,
    errors: Array<OpError>
  ): Promise<Array<Record<string, unknown>>>;
  async allowFilter(
    collName: string,
    operationName: "update",
    docs: Array<ChangeSetUpdate>,
    _props: MethodProps<MongoDatabaseAdapter>,
    errors: Array<OpError>
  ): Promise<Array<ChangeSetUpdate>>;
  async allowFilter(
    collName: string,
    operationName: "remove",
    docs: Array<string>,
    _props: MethodProps<MongoDatabaseAdapter>,
    errors: Array<OpError>
  ): Promise<Array<string>>;
  async allowFilter(
    collName: string,
    operationName: "insert" | "update" | "remove",
    docs:
      | Array<Record<string, unknown>>
      | Array<ChangeSetUpdate>
      | Array<string>,
    _props: MethodProps<MongoDatabaseAdapter>,
    errors: Array<OpError>
  ): Promise<
    Array<Record<string, unknown>> | Array<ChangeSetUpdate> | Array<string>
  > {
    const coll = this.collection(collName);
    const props: CollectionEventProps = {
      collection: coll,
      eventName: "remove",
      ..._props,
    };

    if (operationName === "insert")
      return await coll.allowFilter(
        "insert",
        docs as Document[],
        props,
        errors
      );
    else if (operationName === "update")
      return await coll.allowFilter(
        "update",
        docs as ChangeSetUpdate[],
        props,
        errors
      );
    // if (operationName === "remove")
    else
      return await coll.allowFilter("remove", docs as string[], props, errors);
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
    {
      updatedAt,
      limit,
      sort,
      lastSortedValue,
    }: PublicationProps<MongoDatabaseAdapter>
  ) {
    if (publishResult instanceof Cursor) {
      const collName = publishResult.coll.name;
      if (!publishResult.filter) publishResult.filter = {};
      if (updatedAt && updatedAt[collName]) {
        publishResult.filter.__updatedAt = { $gt: updatedAt[collName] };
        publishResult.sort("__updatedAt", "asc");
        publishResult.limit(200);
      } else {
        if (sort) publishResult.sort(sort[0], sort[1]);
        if (limit) publishResult.limit(limit);
        if (lastSortedValue) {
          if (!sort) throw new Error("lastSortedValue requires sort");
          publishResult.filter[sort[0]] = {
            [sort[1] === "asc" ? "$gt" : "$lt"]: lastSortedValue,
          };
        }
      }
      // console.log(publishResult);

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
