class Cursor {

  constructor(coll, query) {
    this.db = coll.db;
    this.coll = coll;
    this.query = query;
  }
  
  // TODO
  limit(limit) {
    throw new Error("limit not implemented yet")
  }

  async toArray() {
    const db = await this.db.dbPromise;
    const data = await db.collection(this.coll.name).find(this.query).toArray();
    return data;
  }

}

module.exports = Cursor;
