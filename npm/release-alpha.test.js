"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  assertAlphaVersion,
  cargoWorkspaceVersion,
  nextAlphaVersion,
  parseArgs,
  platformPackageNames,
  setCargoLockVersion,
  setCargoWorkspaceVersion,
  setPlatformDependencyVersions,
  workspacePackageNames,
} = require("./release-alpha.js");

test("nextAlphaVersion increments the alpha prerelease number", () => {
  assert.equal(nextAlphaVersion("0.1.0-alpha.1"), "0.1.0-alpha.2");
  assert.equal(nextAlphaVersion("1.2.3-alpha.9"), "1.2.3-alpha.10");
});

test("alpha release validation rejects non-alpha versions", () => {
  assert.doesNotThrow(() => assertAlphaVersion("0.1.0-alpha.2"));
  assert.throws(() => assertAlphaVersion("0.1.0"));
  assert.throws(() => assertAlphaVersion("0.1.0-beta.1"));
});

test("cargo workspace version can be read and replaced", () => {
  const cargoToml = [
    "[workspace]",
    'resolver = "2"',
    "",
    "[workspace.package]",
    'edition = "2024"',
    'version = "0.1.0-alpha.1"',
    "",
  ].join("\n");

  assert.equal(cargoWorkspaceVersion(cargoToml), "0.1.0-alpha.1");
  assert.equal(cargoWorkspaceVersion(setCargoWorkspaceVersion(cargoToml, "0.1.0-alpha.2")), "0.1.0-alpha.2");
});

test("cargo lock version replacement touches only workspace packages", () => {
  const lock = [
    "[[package]]",
    'name = "cc"',
    'version = "1.2.61"',
    "",
    ...workspacePackageNames.flatMap((name) => [
      "[[package]]",
      `name = "${name}"`,
      'version = "0.1.0-alpha.1"',
      "",
    ]),
  ].join("\n");

  const updated = setCargoLockVersion(lock, "0.1.0-alpha.2");

  assert.match(updated, /name = "cc"\nversion = "1\.2\.61"/);
  assert.match(updated, /name = "peerline-cli"\nversion = "0\.1\.0-alpha\.2"/);
  assert.match(updated, /name = "peerline-core"\nversion = "0\.1\.0-alpha\.2"/);
});

test("parseArgs supports publish options and current-version retries", () => {
  const options = parseArgs(["--current", "--otp", "123456", "--tag=alpha", "--access", "public"]);

  assert.equal(options.current, true);
  assert.equal(options.otp, "123456");
  assert.equal(options.tag, "alpha");
  assert.equal(options.access, "public");
});

test("parseArgs rejects mutually exclusive version selectors", () => {
  assert.throws(() => parseArgs(["--current", "--version", "0.1.0-alpha.2"]));
});

test("platform package names do not require a private npm scope", () => {
  assert.ok(platformPackageNames.includes("peerline-linux-x64-gnu"));
  assert.ok(platformPackageNames.includes("peerline-darwin-arm64"));
  assert.equal(platformPackageNames.some((name) => name.startsWith("@peerline/")), false);
});

test("workspace package list covers every local crate in Cargo.lock", () => {
  assert.deepEqual([...workspacePackageNames].sort(), [
    "peerline-cli",
    "peerline-core",
    "peerline-crypto",
    "peerline-net",
    "peerline-testkit",
    "peerline-transfer",
    "peerline-tui",
  ]);
});

test("setPlatformDependencyVersions pins every platform package to the release version", () => {
  const packageJson = setPlatformDependencyVersions({ optionalDependencies: {} }, "0.1.0-alpha.2");

  for (const name of platformPackageNames) {
    assert.equal(packageJson.optionalDependencies[name], "0.1.0-alpha.2");
  }
});
