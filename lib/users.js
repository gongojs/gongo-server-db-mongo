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
    const hash = await this.db.gongoServer.bcryptHash(password);
    await this.users.updateOne({ _id: userId }, { $set: { password: hash }});
  }

  async ensureAdmin(email, password) {
    if (process.env.NO_ENSURE)
      return;

    let user = await this.users.findOne({ emails: { value: email }});
    if (!user) {
      const userId = await this.createUser(user => {
        user.emails.push({ value: email });
      });

      if (userId)
        await this.setUserPassword(userId, password);
    }
  }

  async createUser(callback) {
    const user = {
      emails: [],
      services: []
    };

    if (callback)
      callback(user);

    const result = await this.users.insertOne(user);
    // result.inseterId, result.ops;
    return result.ops[0];
  }

  // TODO, move non-db stuff to to gongo-server
  async findOrCreateService(email, service, id, profile, accessToken, refreshToken) {
    const query = { $or: [] };
    if (email) {
      if (typeof email === 'string') {
        query.$or.push({ 'emails.value': email });
      } else if (Array.isArray(email)) {
        query.$or.push({ 'emails.value':
          { $in: email.map(email => email.value) }})
      } else {
        console.log("Ignoring unknown email type"
          + typeof email + ' ' + JSON.stringify(email));
      }
    }
    if (service)
      query.$or.push({ [`service.${service}.id`]: id });

    let user = await this.users.findOne(query);

    if (user) {

      // Update service info & add any missing fields

      user.services = user.services.filter(s => s.id !== id);
      user.services.push({ service, id, profile, accessToken, refreshToken });

      if (!user.displayName)
        user.displayName = profile.displayName;

      if (!user.name)
        user.name = profile.name;

      profile.emails.forEach(email => {
        if (!profile.emails.find(e => e.value === email.value))
          profile.emails.push(email);
      });

      if (!user.photos)
        user.photos = [];
      user.photos = user.photos.filter(
        photo => photo.provider !== profile.provider
      );
      profile.photos.forEach(photo => {
        photo.provider = profile.provider; user.photos.push(photo)
      });

      if (!user.gender && (profile.gender || profile._json.gender))
        user.gender = profile.gender || profile._json.gender;

      if (!user.gender && (profile.locale || profile._json.locale))
        user.locale = profile.locale || profile._json.locale

      await this.users.replaceOne({ _id: user._id }, user);

    } else {

      // Create new user

      user = await this.createUser(user => {
        user.services.push({ service, id, profile, accessToken, refreshToken });

        user.displayName = profile.displayName;
        user.name = profile.name;
        user.emails = profile.emails;
        user.photos = profile.photos;
        user.photos.forEach(photo => photo.provider = profile.provider);

        if (profile.gender || profile._json.gender)
          user.gender = profile.gender || profile._json.gender;

        if (profile.locale || profile._json.locale)
          user.locale = profile.locale || profile._json.locale
      });

    }

    return user;
  }

}

module.exports = { __esModule: true, default: Users };
