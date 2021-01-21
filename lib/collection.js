//const toMongoDb = require('jsonpatch-to-mongodb');
const toMongoDb = require('./jsonpatch-to-mongodb');
const Cursor = require('./cursor');

class Collection {

  constructor(db, name) {
    this.db = db;
    this.name = name;
    this._indexCreated = false;
  }

  async getReal() {
    const db = await this.db.dbPromise;
    const realColl = db.collection(this.name);

    // TODO is serverless the best place for this?
    if (!this._indexCreated) {
      this._indexCreated = true;

      // don't await.
      realColl.createIndex('__updatedAt');
    }

    return realColl;
  }

  find(query) {
    // deal with __updatedAts
    // if NO __updatedAt specified, should NOT include deleted records
    //   (because we're getting data for first time!)

    return new Cursor(this, query);
  }

  async findOne(query) {
    const realColl = await this.getReal();
    return await realColl.findOne(query);
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

  async replaceOne(query, doc, options) {
    const realColl = await this.getReal();

    if (!doc)
      throw new Error("not replacing " + query + " with empty doc");

    doc.__updatedAt = Date.now();
    return realColl.replaceOne(query, doc, options);
  }

  async updateOne(query, update = {}, options) {
    const realColl = await this.getReal();

    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();

    return realColl.updateOne(query, update, options);
  }

  async applyPatches(entries) {
    const realColl = await this.getReal();
    const bulk = [];

    for (let entry of entries) {
      console.log('patch', entry.patch);
      const update = toMongoDb(entry.patch);
      console.log('update', update);
      if (!update.$set) update.$set = {};
      update.$set.__updatedAt = Date.now();

      bulk.push({
        updateOne: {
          filter: { _id: entry._id },
          update,
        }
      });
    }

    console.log(JSON.stringify(bulk, null, 2));
    await realColl.bulkWrite(bulk);
  }

  async applyPatch(entry) {
    const _id = entry._id;
    const update = toMongoDb(entry.patch);

    if (!update.$set) update.$set = {};
    update.$set.__updatedAt = Date.now();

    console.log('patch', entry, update);
    await this.updateOne(_id, update);
  }

}

module.exports = Collection;
