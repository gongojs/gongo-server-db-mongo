import { ObjectId } from "bson";
import type { MatcherFunction } from "expect";
import { expect } from "@jest/globals";

const toHaveObjectId: MatcherFunction<[id: string | number]> = function (
  actual,
  id
) {
  if (!(actual instanceof ObjectId))
    throw new Error(
      "Received value should be ObjectId, not " +
        typeof ObjectId +
        ": " +
        JSON.stringify(ObjectId)
    );
  if (typeof id !== "string" && !Number.isInteger(id))
    throw new Error(
      "Expected value should be a string or integer, not " +
        typeof id +
        ": " +
        JSON.stringify(id)
    );
  return {
    pass: actual.toHexString() === id,
    message: () =>
      `expected ${this.utils.printReceived(
        actual
      )} to be ObjectID(${this.utils.printExpected(id)})`,
  };
};

expect.extend({
  toHaveObjectId,
});

/*
declare module "expect" {
  interface AsymmetricMatchers {
    toHaveObjectId(id: string): void;
  }
  interface Matchers<R> {
    toHaveObjectId(id: string): R;
  }
}
*/

declare global {
  // eslint-disable-next-line
  namespace jest {
    interface Matchers<R> {
      toHaveObjectId(id: string): R;
    }
    interface Expect {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      toHaveObjectId(id: string): any;
    }

    interface InverseAsymmetricMatchers {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      toHaveObjectId(id: string): any;
    }
  }
}
