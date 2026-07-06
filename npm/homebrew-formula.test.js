"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  defaultReleaseBaseUrl,
  formulaAssets,
  generateHomebrewFormula,
  normalizeReleaseBaseUrl,
  parseArgs,
  parseChecksumLine,
  readChecksums,
  repositorySlug,
} = require("./homebrew-formula.js");

function fakeChecksums() {
  const digits = ["a", "b", "c", "d"];
  return Object.fromEntries(formulaAssets.map((asset, index) => [asset, digits[index].repeat(64)]));
}

test("generateHomebrewFormula points every supported platform at release assets", () => {
  const formula = generateHomebrewFormula({
    checksums: fakeChecksums(),
    releaseBaseUrl: "https://example.invalid/releases/v0.1.0/",
    version: "0.1.0",
  });

  assert.match(formula, /class Peerline < Formula/);
  assert.match(formula, /version "0\.1\.0"/);
  assert.match(formula, /on_macos do/);
  assert.match(formula, /on_linux do/);
  for (const asset of formulaAssets) {
    assert.match(formula, new RegExp(`url "https://example\\.invalid/releases/v0\\.1\\.0/${asset}", using: :nounzip`));
  }
  assert.match(formula, /sha256 "a{64}"/);
  assert.match(formula, /bin\.install asset => "peerline"/);
  assert.match(formula, /shell_output\("#\{bin\}\/peerline --version"\)/);
});

test("readChecksums reads shasum output and rejects mismatched assets", (t) => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-homebrew-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const checksums = fakeChecksums();
  for (const asset of formulaAssets) {
    fs.writeFileSync(path.join(tempDir, `${asset}.sha256`), `${checksums[asset]}  ${asset}\n`);
  }

  assert.deepEqual(readChecksums(tempDir), checksums);

  fs.writeFileSync(
    path.join(tempDir, `${formulaAssets[0]}.sha256`),
    `${checksums[formulaAssets[0]]}  other-asset\n`
  );
  assert.throws(() => readChecksums(tempDir), /expected peerline-darwin-arm64/);
});

test("parseChecksumLine accepts binary-mode checksum lines", () => {
  assert.deepEqual(parseChecksumLine(`${"f".repeat(64)} *peerline-linux-x64-gnu`, "sample.sha256"), {
    assetName: "peerline-linux-x64-gnu",
    sha256: "f".repeat(64),
  });
});

test("repository and release URL helpers use the package GitHub repository", () => {
  assert.equal(repositorySlug({ repository: { url: "https://github.com/backrunner/peerline" } }), "backrunner/peerline");
  assert.equal(repositorySlug({ repository: "git+https://github.com/backrunner/peerline.git" }), "backrunner/peerline");
  assert.equal(
    defaultReleaseBaseUrl("0.1.0", { repository: { url: "https://github.com/backrunner/peerline" } }),
    "https://github.com/backrunner/peerline/releases/download/v0.1.0"
  );
  assert.equal(normalizeReleaseBaseUrl("https://example.invalid/releases/"), "https://example.invalid/releases");
});

test("parseArgs supports dist, output, version, and release base URL", () => {
  const options = parseArgs([
    "--dist",
    "dist-assets",
    "--output=Formula/peerline.rb",
    "--version",
    "0.1.0-beta.1",
    "--release-base-url=https://example.invalid/v0.1.0-beta.1",
  ]);

  assert.equal(options.distDir, path.resolve("dist-assets"));
  assert.equal(options.output, path.resolve("Formula/peerline.rb"));
  assert.equal(options.version, "0.1.0-beta.1");
  assert.equal(options.releaseBaseUrl, "https://example.invalid/v0.1.0-beta.1");
});
