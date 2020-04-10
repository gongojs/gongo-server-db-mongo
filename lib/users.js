class Users {

  constructor(db) {
    this.db = db;
    this.users = this.db.collection('users');  // TODO custom name
  }

}

module.exports = { __esModule: true, default: Users };
