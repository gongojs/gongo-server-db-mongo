module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  testPathIgnorePatterns: ["<rootDir>/lib/", "<rootDir>/node_modules/"],
  setupFilesAfterEnv: ["<rootDir>/setup-matchers.ts"],
};
