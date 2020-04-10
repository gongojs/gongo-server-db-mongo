class Users {

  constructor(db) {
    this.db = db;

    // TODO custom names
    this.users = db.collection('users');
    this.sessions = db.collection('sessions');
  }

  async setSessionData(sid, data) {
    await this.sessions.updateOne(
      { _id: sid },
      { $set: { data }},
      { upsert: true }
    );
  }

  async getUserWithEmailAndPassword(email, password) {
    const user = await this.users.findOne({ emails: { email }});
    if (!user) return null;

    if (await this.db.gongoServer.bcryptCompare(password, user.password))
      return user;
    else
      return null;
  }

  async setUserPassword(userId, password) {
    console.log(password);
    const hash = await this.db.gongoServer.bcryptHash(password);
    console.log(hash);
    await this.users.updateOne({ _id: userId }, { $set: { password: hash }});
  }

  async ensureAdmin(email, password) {
    let user = await this.users.findOne({ emails: { email }});
    if (!user) {
      const result = await this.users.insertOne({
        isAdmin: true,
        emails: [ { email } ],
        providers: [],
      });

      let userId = result.insertedId;
      if (userId) {
        await this.setUserPassword(userId, password);
      }
    }
  }

}

module.exports = { __esModule: true, default: Users };
