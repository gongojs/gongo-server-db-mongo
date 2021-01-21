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

module.exports = Cursor;
