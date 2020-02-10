const MongoClient = require('mongodb').MongoClient;

class Database {

  constructor(url) {
    const dbName = 'gongo';
    const client = this.client = new MongoClient(url);

    this.dbPromise = new Promise((resolve, reject) => {
      client.connect(err => {
        if (err) reject(err);
        resolve(client.db('gongo'));
      });
    });
  }

  collection(name) {
    if (!this.collection[name])
      this.collection[name] = new Collection(this, name);

    return this.collection[name];
  }

  async processChangeSet(changeSet) {
    for (let [collName, ops] of Object.entries(changeSet)) {
      const coll = this.collection(collName);

      if (ops.insert)
        coll.insertMany(ops.insert);

      if (ops.delete)
        coll.markAsDeleted(ops.delete);

    }
  }

  async publishHelper(publishResult) {
    if (publishResult instanceof Cursor) {
      return [{
        coll: publishResult.coll.name,
        entries: await publishResult.toArray()
      }];
    } else {
      return publishResult;
    }
  }

}

class Collection {

  constructor(db, name) {
    this.db = db;
    this.name = name;
  }

  async getReal() {
    const db = await this.db.dbPromise;
    return db.collection(this.name);
  }

  find(query) {
    // deal with __updatedAts
    // if NO __updatedAt specified, should NOT include deleted records
    //   (because we're getting data for first time!)

    return new Cursor(this, query);
  }

  async insertOne(doc) {
    const realColl = await this.getReal();

    doc.__updatedAt = Date.now();
    console.log(this.name+' insert: ' + JSON.stringify(doc));
    return await realColl.insertOne(doc);
  }

  /*
  insert existing id should fail silently
  delete a non-existing id should fail silently
  Fow now.  think if these errors should go back to client.
  In bigger scheme of things, all we care is that client
  gets correct server copy
   */

  async insertMany(docArray) {
    const realColl = await this.getReal();

    for (let doc of docArray)
      doc.__updatedAt = Date.now();

    console.log(this.name+' insertMany: ' + JSON.stringify(docArray));
    //return await realColl.insertMany(docArray, { ordered: false /* XXX TODO */ });

    await realColl.bulkWrite(
      docArray.map(doc => ({
        replaceOne: {
          filter: { _id: doc._id },
          update: { $setOnInsert: doc },
          upsert: true /* XXX TODO */
        }
      })
    ));
  }

  async markAsDeleted(idArray) {
    const realColl = await this.getReal();
    console.log(this.name+' markAsDeleted: ' + idArray.join(','));

    await realColl.bulkWrite(
      idArray.map(id => ({
        replaceOne: {
          filter: { _id: id },
          replacement: { _id: id, __deleted: true, __updatedAt: Date.now() },
          upsert: true /* XXX TODO */
        }
      })
    ));
  }

}

class Cursor {

  constructor(coll, query) {
    this.db = coll.db;
    this.coll = coll;
    this.query = query;
  }

  async toArray() {
    const db = await this.db.dbPromise;
    const data = await db.collection(this.coll.name).find(this.query).toArray();
    return data;
  }

}

module.exports = { __esModule: true, default: Database };
