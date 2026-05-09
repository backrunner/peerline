"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {
  assertAlphaVersion,
  assertReleaseVersion,
  cargoWorkspaceVersion,
  distTagForChannel,
  nextAlphaVersion,
  nextReleaseVersion,
  normalizeChannel,
  parseArgs,
  platformPackageInfo,
  platformPackageNames,
  platformPackageSpecs,
  repository,
  setCargoLockVersion,
  setCargoWorkspaceVersion,
  setPlatformDependencyVersions,
  workspacePackageNames,
} = require("./release.js");

test("nextAlphaVersion increments the alpha prerelease number", () => {
  assert.equal(nextAlphaVersion("0.1.0-alpha.1"), "0.1.0-alpha.2");
  assert.equal(nextAlphaVersion("1.2.3-alpha.9"), "1.2.3-alpha.10");
});

test("nextReleaseVersion advances alpha, beta, and stable channels", () => {
  assert.equal(nextReleaseVersion("0.1.0-alpha.2", "beta"), "0.1.0-beta.0");
  assert.equal(nextReleaseVersion("0.1.0-beta.0", "beta"), "0.1.0-beta.1");
  assert.equal(nextReleaseVersion("0.1.0-beta.1", "stable"), "0.1.0");
  assert.equal(nextReleaseVersion("0.1.0", "stable"), "0.1.1");
});

test("alpha release validation rejects non-alpha versions", () => {
  assert.doesNotThrow(() => assertAlphaVersion("0.1.0-alpha.2"));
  assert.throws(() => assertAlphaVersion("0.1.0"));
  assert.throws(() => assertAlphaVersion("0.1.0-beta.1"));
});

test("release channel validation and dist-tags match npm conventions", () => {
  assert.equal(normalizeChannel("release"), "stable");
  assert.equal(distTagForChannel("alpha"), "alpha");
  assert.equal(distTagForChannel("beta"), "beta");
  assert.equal(distTagForChannel("stable"), "latest");
  assert.doesNotThrow(() => assertReleaseVersion("0.1.0-beta.0", "beta"));
  assert.doesNotThrow(() => assertReleaseVersion("0.1.0", "stable"));
  assert.throws(() => assertReleaseVersion("0.1.0-alpha.0", "beta"));
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
  const options = parseArgs([
    "--channel=beta",
    "--current",
    "--otp",
    "123456",
    "--access",
    "public",
    "--platform-package",
    "peerline-linux-arm64-musl",
    "--ignore-existing",
  ]);

  assert.equal(options.channel, "beta");
  assert.equal(options.current, true);
  assert.equal(options.otp, "123456");
  assert.equal(options.tag, "beta");
  assert.equal(options.access, "public");
  assert.equal(options.platformPackage, "peerline-linux-arm64-musl");
  assert.equal(options.ignoreExisting, true);
});

test("parseArgs rejects unknown platform packages", () => {
  assert.throws(() => parseArgs(["--platform-package", "peerline-missing"]));
});

test("parseArgs rejects mutually exclusive version selectors", () => {
  assert.throws(() => parseArgs(["--current", "--version", "0.1.0-alpha.2"]));
});

test("platform package names do not require a private npm scope", () => {
  assert.ok(platformPackageNames.includes("peerline-linux-x64-gnu"));
  assert.ok(platformPackageNames.includes("peerline-linux-arm64-gnu"));
  assert.ok(platformPackageNames.includes("peerline-linux-arm64-musl"));
  assert.ok(platformPackageNames.includes("peerline-darwin-arm64"));
  assert.equal(platformPackageNames.some((name) => name.startsWith("@peerline/")), false);
});

test("platform package specs expose publish-time build metadata", () => {
  assert.equal(platformPackageSpecs["peerline-linux-arm64-musl"].cargoTarget, "aarch64-unknown-linux-musl");
  assert.equal(platformPackageSpecs["peerline-linux-x64-musl"].libcField[0], "musl");
  assert.equal(platformPackageSpecs["peerline-linux-arm64-gnu"].libcField[0], "glibc");
});

test("trusted publishing keeps GitHub repository metadata aligned", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"));

  assert.deepEqual(packageJson.repository, repository);
  assert.equal(packageJson.repository.url, "https://github.com/peerline/peerline");
  assert.equal(packageJson.publishConfig?.tag, undefined);
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

test("platformPackageInfo resolves names to host package metadata", () => {
  const info = platformPackageInfo("peerline-linux-arm64-musl", "0.1.0-beta.1");

  assert.equal(info.name, "peerline-linux-arm64-musl");
  assert.equal(info.platform, "linux");
  assert.equal(info.arch, "arm64");
  assert.equal(info.libc, "musl");
  assert.equal(info.cargoTarget, "aarch64-unknown-linux-musl");
  assert.equal(info.binaryName, "peerline");
});
