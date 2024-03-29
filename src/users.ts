import type {
  DbaUserEmail,
  DbaUserService,
  DbaUsers,
  Profile,
} from "gongo-server/lib/DatabaseAdapter.js";
import { ObjectId } from "./objectid";
import type { Document, Filter, WithId } from "mongodb";

import type DatabaseAdapter from "./databaseAdapter.js";
import type { MongoDbaUser } from "./databaseAdapter.js";
import type Collection from "./collection.js";
import type { GongoDocument } from "./collection.js";

interface SessionData extends GongoDocument {
  [key: string]: unknown;
  _id: string;

  userId: ObjectId;
  ip?: string;
  userAgent?: string;

  sessionToken?: string;
  expires: Date;
}

export default class Users implements DbaUsers {
  dba: DatabaseAdapter;
  users: Collection;
  sessions: Collection<SessionData>;

  constructor(dba: DatabaseAdapter) {
    this.dba = dba;

    // TODO custom names
    this.users = dba.collection("users");
    this.sessions = dba.collection<SessionData>("sessions");
  }

  async setSessionData(sid: string, data: Record<string, unknown>) {
    await this.sessions.updateOne(
      { $or: [{ _id: sid }, { sessionToken: sid }] },
      { $set: data },
      { upsert: true },
    );
  }

  async getSessionData(sid: string) {
    return await this.sessions.findOne({
      $or: [{ _id: sid }, { sessionToken: sid }],
    });
  }

  async getUserWithEmailAndPassword() {
    throw new Error("not implemented yet");
    return null;
  }

  /*
  async getUserWithEmailAndPassword(email: string, password: string) {
    const user = await this.users.findOne({ emails: { email } });
    if (!user) return null;

    if (await this.dba.gs.bcryptCompare(password, user.password))
      return user;
    else return null;
  }

  async setUserPassword(userId, password) {
    const hash = await this.db.gongoServer.bcryptHash(password);
    await this.users.updateOne({ _id: userId }, { $set: { password: hash } });
  }

  async ensureAdmin(email:string, password:string) {
    if (process.env.NO_ENSURE) return;

    let user = await this.users.findOne({ emails: { value: email } });
    if (!user) {
      user = await this.createUser((user: DbaUser) => {
        user.emails.push({ value: email });
        user.admin = true;
      });

      if (user._id) await this.setUserPassword(user._id, password);
    }
  }
  */

  async createUser(
    callback?: (dbaUser: Partial<MongoDbaUser>) => void,
  ): Promise<MongoDbaUser> {
    const user: Partial<MongoDbaUser> = {
      emails: [],
      services: [],
    };

    if (callback) callback(user);

    const result = await this.users.insertOne(user);
    // This doesn't always work, must be multiple copies of bson package
    if (result.acknowledged && result.insertedId /* instanceof ObjectId */) {
      user._id = result.insertedId as ObjectId; // we shouldn't need "as" here?
      return user as MongoDbaUser;
    } else {
      // console.log(result);
      throw new Error(
        "Unexpected mongo result in createUser():" + JSON.stringify(result),
      );
    }
  }

  // TODO, move non-db stuff to to gongo-server
  async findOrCreateService(
    email: string | Array<DbaUserEmail> | undefined,
    service: string,
    id: string,
    profile: Profile,
    accessToken: string,
    refreshToken: string,
  ): Promise<MongoDbaUser> {
    const filter: Filter<Document> = { $or: [] };
    const $or = filter.$or as Filter<WithId<Document>>[];
    if (email) {
      if (typeof email === "string") {
        $or.push({ "emails.value": email });
      } else if (Array.isArray(email)) {
        $or.push({
          "emails.value": { $in: email.map((email) => email.value) },
        });
      } else {
        console.log(
          "Ignoring unknown email type" +
            typeof email +
            " " +
            JSON.stringify(email),
        );
      }
    }
    if (service)
      $or.push({
        $and: [{ "services.service": service }, { "services.id": id }],
      });

    let user = (await this.users.findOne(filter)) as MongoDbaUser | null;

    if (user) {
      // Update service info & add any missing fields

      user.services = user.services.filter(
        (s: DbaUserService) => !(s.service === service && s.id === id),
      );
      user.services.push({ service, id, profile, accessToken, refreshToken });

      if (!user.displayName) user.displayName = profile.displayName;

      if (!user.name) user.name = profile.name;

      if (!user.emails) user.emails = [];

      // TODO, update "verified" field, make sure only one is "primary"
      if (Array.isArray(profile.emails)) {
        for (const email of profile.emails)
          if (!user.emails.includes(email)) user.emails.push(email);
      }

      if (!user.photos) user.photos = [];
      user.photos = user.photos.filter(
        (photo: Record<string, string>) => photo.provider !== profile.provider,
      );
      if (profile.photos) {
        for (const photo of profile.photos) {
          user.photos.push({ ...photo, provider: profile.provider });
        }
      }

      // @ts-expect-error: purposefully inspect at runtime
      if (!user.gender && (profile.gender || profile._json.gender))
        // @ts-expect-error: purposefully inspect at runtime
        user.gender = profile.gender || profile._json.gender;

      // @ts-expect-error: purposefully inspect at runtime
      if (!user.gender && (profile.locale || profile._json.locale))
        // @ts-expect-error: purposefully inspect at runtime
        user.locale = profile.locale || profile._json.locale;

      await this.users.replaceOne({ _id: user._id }, user);
    } else {
      // Create new user

      user = await this.createUser((user: Partial<MongoDbaUser>) => {
        if (!user.services) user.services = [];
        user.services.push({ service, id, profile, accessToken, refreshToken });

        user.displayName = profile.displayName;
        user.name = profile.name;
        user.emails = profile.emails || [];
        if (profile.photos)
          user.photos = profile.photos.map((photo) => ({
            ...photo,
            provider: profile.provider,
          }));

        // @ts-expect-error: purposefully inspect at runtime
        if (profile.gender || profile._json.gender)
          // @ts-expect-error: purposefully inspect at runtime
          user.gender = profile.gender || profile._json.gender;

        // @ts-expect-error: purposefully inspect at runtime
        if (profile.locale || profile._json.locale)
          // @ts-expect-error: purposefully inspect at runtime
          user.locale = profile.locale || profile._json.locale;
      });
    }

    return user;
  }
}
