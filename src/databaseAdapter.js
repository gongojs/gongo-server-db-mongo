const _MongoClient = require("mongodb").MongoClient;
const ObjectID = require("mongodb").ObjectID;

const Cursor = require("./cursor");
const Collection = require("./collection");
const Users = require("./users");

class MongoDatabaseAdapter {
  constructor(url, dbName = "gongo", MongoClient = _MongoClient) {
    const client = (this.client = new MongoClient(url, {
      useUnifiedTopology: true,
    }));

    this.dbPromise = new Promise((resolve, reject) => {
      client.connect((err) => {
        if (err) reject(err);
        resolve(client.db(dbName));
      });
    });

    this.collections = {};
    this.Users = new Users(this);
  }

  onInit(gs) {
    const ARSON = gs.ARSON;

    // ARSON is actually called in gongo-server
    ARSON.registerType("ObjectID", {
      deconstruct: function (id) {
        //return id instanceof ObjectID && [ id.toHexString() ];

        if (id instanceof ObjectID) {
          // console.log(id, "instanceof ObjectID ==", id instanceof ObjectID);
          return [id.toHexString()];
        }

        if (typeof id === "object") {
          // Not quite sure in what circumstances this happens,
          // I guess multiple versions of ObjectID in use somehow.
          const keys = Object.keys(id); // [ '_bsontype', 'id' ]
          // console.log(id, keys, true);
          if (keys[0] === "_bsontype" && keys[1] === "id")
            return [id.toHexString()];
        }

        return false;
      },

      reconstruct: function (args) {
        return args && ObjectID.createFromHexString(args[0]);
      },
    });
  }

  collection(name) {
    if (!this.collections[name])
      this.collections[name] = new Collection(this, name);

    return this.collections[name];
  }

  async processChangeSet(changeSet /* { auth, req } */) {
    for (let [collName, ops] of Object.entries(changeSet)) {
      const coll = this.collection(collName);

      if (ops.insert) await coll.insertMany(ops.insert);

      if (ops.update) {
        await coll.applyPatches(ops.update);
      }

      if (ops.delete) await coll.markAsDeleted(ops.delete);
    }
  }

  async publishHelper(publishResult, updatedAt, auth, req) {
    if (publishResult instanceof Cursor) {
      const collName = publishResult.coll.name;
      if (!publishResult.query) publishResult.query = {};
      if (updatedAt && updatedAt[collName])
        publishResult.query.__updatedAt = { $gt: updatedAt[collName] };

      const out = [];
      const entries = await publishResult.toArray();
      if (entries.length) out.push({ coll: collName, entries });
      return out;
    } else {
      return publishResult;
    }
  }
}

// At time of writing, Cursor / Collection was only used by Database class
// and exported just for testing.
module.exports = {
  __esModule: true,
  default: MongoDatabaseAdapter,
  ObjectID,
  Collection,
  Cursor,
};
