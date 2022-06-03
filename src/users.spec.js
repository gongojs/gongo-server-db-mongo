const Users = require('./users');
const Collection = require('./collection');

describe('Users', () => {

  describe('constructor', () => {

    it('sets instance vars', () => {
      const dba = { collection(name) { return name } };
      const users = new Users(dba);
      expect(users.db).toBe(dba);
      expect(users.users).toBe('users');
      expect(users.sessions).toBe('sessions');
    });

  });

  describe('setSessionData', () => {

    it('sets data for sid', async () => {
      const dba = { collection(name) { return { updateOne: jest.fn() } }};
      const users = new Users(dba);

      const data = {};
      await users.setSessionData('sid', data);

      expect(users.sessions.updateOne).toHaveBeenCalledWith(
        { _id: 'sid' },
        { $set: data },
        { upsert: true }
      );
    });

  });

  describe('getSesionData', () => {

    it('returns data for sid', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);

      users.sessions.findOne.mockReturnValueOnce({ _id: 'sid', isAdmin: true });
      const data = await users.getSessionData('sid');

      expect(users.sessions.findOne).toHaveBeenCalledWith({ _id: 'sid' });
    });

  });

  describe('getUserWithEmailAndPassword', () => {

    it('returns null on no user with that email', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);

      users.users.findOne.mockReturnValueOnce();
      const user = await users.getUserWithEmailAndPassword('user', 'password');
      expect(user).toBe(null);
    });

    it('returns matching user if password compares true', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);
      const userDoc = { _id: 'id' };

      users.users.findOne.mockReturnValueOnce(userDoc);
      dba.gongoServer = { bcryptCompare: jest.fn() };

      dba.gongoServer.bcryptCompare.mockReturnValueOnce(true);
      const user = await users.getUserWithEmailAndPassword('user', 'password');
      expect(user).toBe(userDoc);
    });

    it('returns null if password compares false', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);
      const userDoc = { _id: 'id' };

      users.users.findOne.mockReturnValueOnce(userDoc);
      dba.gongoServer = { bcryptCompare: jest.fn() };

      dba.gongoServer.bcryptCompare.mockReturnValueOnce(false);
      const user = await users.getUserWithEmailAndPassword('user', 'password');
      expect(user).toBe(null);
    });

  });

  describe('setUserPassword', () => {

    it('updateOnes userId with hash from bcrypt', async () => {
      const dba = { collection(name) { return { updateOne: jest.fn() } }};
      const users = new Users(dba);

      dba.gongoServer = { bcryptHash: jest.fn() };
      dba.gongoServer.bcryptHash.mockReturnValueOnce('hash');

      await users.setUserPassword('userId', 'password');
      expect(users.users.updateOne).toHaveBeenCalledWith(
        { _id: 'userId' },
        { $set: { password: 'hash' }}
      );
    });

  });

  describe('ensureAdmin', () => {

    it('does nothing if NO_ENSURE set', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);

      process.env.NO_ENSURE = true;
      await users.ensureAdmin('email', 'password');
      delete process.env.NO_ENSURE;

      expect(users.users.findOne).not.toHaveBeenCalledWith();
    });

    it('creates user if does not already exist', async () => {
      const dba = { collection(name) { return { findOne: jest.fn() } }};
      const users = new Users(dba);

      let _user;
      users.users.findOne.mockReturnValueOnce();
      users.createUser = jest.fn().mockImplementation(async (callback) => {
        const user = _user = { _id: 'userId', emails: [] };
        callback(user);
        return user;
      });
      users.setUserPassword = jest.fn();

      await users.ensureAdmin('email', 'password');

      expect(users.createUser).toHaveBeenCalled();
      expect(_user.emails).toContainEqual({ value: 'email' });
      expect(users.setUserPassword).toHaveBeenCalledWith('userId', 'password');
    });

  });

  describe('createUser', () => {

    it('creates a stub user and returns with inserted id', async () => {
      const dba = { collection(name) { return { insertOne: jest.fn() } }};
      const users = new Users(dba);

      // http://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertOneWriteOpResult
      users.users.insertOne.mockImplementation(doc => {
        doc._id = 'insertedId';
        return { ops: [ doc ] };
      });

      //const callback = user => user.didSomething = true;
      const user = await users.createUser();
      expect(user).toEqual({ _id: 'insertedId', emails: [], services: [] });
    });

    it('runs callback if given before inserting user', async () => {
      const dba = { collection(name) { return { insertOne: jest.fn() } }};
      const users = new Users(dba);

      // http://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertOneWriteOpResult
      users.users.insertOne.mockImplementation(doc => {
        doc._id = 'insertedId';
        return { ops: [ doc ] };
      });

      const doSomething = user => user.didSomething = true;
      const user = await users.createUser(doSomething);
      expect(user.didSomething).toBe(true);
    });

  });

  describe('findOrCreateService', () => {

    it('', async () => {

    });

  });


});
