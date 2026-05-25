"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  downloadReleaseBinary,
  cachedBinaryPath,
  detectLibc,
  ensureExecutable,
  executeBinary,
  exitCodeForResult,
  packageName,
  packageVersion,
  releaseAssetName,
  releaseAssetUrl,
} = require("./peerline.js");

test("packageName uses unscoped platform packages", () => {
  assert.equal(packageName("darwin", "arm64"), "peerline-darwin-arm64");
  assert.equal(packageName("darwin", "x64"), "peerline-darwin-x64");
  assert.equal(packageName("linux", "x64", "gnu"), "peerline-linux-x64-gnu");
  assert.equal(packageName("linux", "x64", "musl"), "peerline-linux-x64-musl");
  assert.equal(packageName("linux", "arm64", "gnu"), "peerline-linux-arm64-gnu");
  assert.equal(packageName("linux", "arm64", "musl"), "peerline-linux-arm64-musl");
  assert.equal(packageName("win32", "x64"), "peerline-win32-x64-msvc");
});

test("detectLibc distinguishes glibc and musl reports", { skip: process.platform !== "linux" }, () => {
  assert.equal(detectLibc(() => ({ header: { glibcVersionRuntime: "2.39" } })), "gnu");
  assert.equal(detectLibc(() => ({ header: {} })), "musl");
});

test("release assets are named for direct GitHub downloads", () => {
  assert.equal(releaseAssetName("peerline-linux-x64-gnu", "peerline"), "peerline-linux-x64-gnu");
  assert.equal(releaseAssetName("peerline-win32-x64-msvc", "peerline.exe"), "peerline-win32-x64-msvc.exe");
  assert.equal(
    releaseAssetUrl("0.1.0-alpha.6", {
      PEERLINE_DOWNLOAD_BASE_URL: "https://example.invalid/releases/v0.1.0-alpha.6/",
    }),
    `https://example.invalid/releases/v0.1.0-alpha.6/${releaseAssetName()}`
  );
});

test("cachedBinaryPath honors PEERLINE_BINARY_CACHE", () => {
  const cachePath = cachedBinaryPath("0.1.0-alpha.6", { PEERLINE_BINARY_CACHE: "/tmp/peerline-cache" }, os);
  assert.equal(cachePath, path.join("/tmp/peerline-cache", "0.1.0-alpha.6", releaseAssetName()));
});

test("downloadReleaseBinary prunes stale cached versions when current binary already exists", async (t) => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const env = { PEERLINE_BINARY_CACHE: tempDir };
  const version = packageVersion();
  const currentBinary = cachedBinaryPath(version, env, os);
  const staleBinary = cachedBinaryPath("0.0.0-stale", env, os);

  fs.mkdirSync(path.dirname(currentBinary), { recursive: true });
  fs.writeFileSync(currentBinary, "current", { mode: 0o755 });
  fs.mkdirSync(path.dirname(staleBinary), { recursive: true });
  fs.writeFileSync(staleBinary, "stale", { mode: 0o755 });

  const resolved = await downloadReleaseBinary({ env, fs, os });

  assert.equal(resolved, currentBinary);
  assert.deepEqual(fs.readdirSync(tempDir), [version]);
  assert.equal(fs.existsSync(currentBinary), true);
});

test("downloadReleaseBinary prunes stale cached versions after downloading the current binary", async (t) => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const env = { PEERLINE_BINARY_CACHE: tempDir };
  const version = packageVersion();
  const currentBinary = cachedBinaryPath(version, env, os);
  const staleBinary = cachedBinaryPath("0.0.0-stale", env, os);

  fs.mkdirSync(path.dirname(staleBinary), { recursive: true });
  fs.writeFileSync(staleBinary, "stale", { mode: 0o755 });

  const resolved = await downloadReleaseBinary({
    env,
    fs,
    os,
    async downloadToFile(url, destination) {
      assert.equal(url, releaseAssetUrl(version, env));
      fs.writeFileSync(destination, "downloaded");
    },
  });

  assert.equal(resolved, currentBinary);
  assert.deepEqual(fs.readdirSync(tempDir), [version]);
  assert.equal(fs.existsSync(currentBinary), true);

  if (process.platform !== "win32") {
    assert.notEqual(fs.statSync(currentBinary).mode & 0o111, 0);
  }
});

test(
  "ensureExecutable restores execute permission for bundled binaries",
  { skip: process.platform === "win32" },
  (t) => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
    t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

    const scriptPath = path.join(tempDir, "peerline");
    fs.writeFileSync(scriptPath, "#!/bin/sh\nexit 0\n", { mode: 0o644 });
    fs.chmodSync(scriptPath, 0o644);

    ensureExecutable(scriptPath);

    const stat = fs.statSync(scriptPath);
    assert.notEqual(stat.mode & 0o111, 0);

    const result = spawnSync(scriptPath, [], { stdio: "pipe" });
    assert.equal(result.error, undefined);
    assert.equal(result.status, 0);
  }
);

test(
  "executeBinary falls back to a temporary copy when the original path cannot execute",
  { skip: process.platform === "win32" },
  (t) => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
    t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

    const scriptPath = path.join(tempDir, "peerline");
    fs.writeFileSync(scriptPath, "#!/bin/sh\nexit 0\n", { mode: 0o644 });
    fs.chmodSync(scriptPath, 0o644);

    const spawnCalls = [];
    let fallbackRoot = null;
    let chmodAttempts = 0;

    const denied = () => {
      const error = new Error("permission denied");
      error.code = "EACCES";
      return error;
    };

    const fakeFs = {
      accessSync() {
        throw denied();
      },
      statSync() {
        return { mode: 0o644 };
      },
      chmodSync(target, mode) {
        chmodAttempts += 1;
        if (chmodAttempts === 1) {
          throw Object.assign(new Error("chmod denied"), { code: "EPERM" });
        }
        fs.chmodSync(target, mode);
      },
      copyFileSync: fs.copyFileSync.bind(fs),
      mkdtempSync(prefix) {
        fallbackRoot = fs.mkdtempSync(prefix);
        return fallbackRoot;
      },
      rmSync: fs.rmSync.bind(fs),
    };

    const result = executeBinary(scriptPath, ["recv", "foo"], {
      fs: fakeFs,
      os,
      spawnSync(binaryPath, argv) {
        spawnCalls.push({ binaryPath, argv });
        if (binaryPath === scriptPath) {
          return { error: denied() };
        }
        return { status: 0 };
      },
    });

    assert.equal(result.status, 0);
    assert.equal(spawnCalls.length, 2);
    assert.equal(spawnCalls[0].binaryPath, scriptPath);
    assert.equal(spawnCalls[1].argv[0], "recv");
    assert.ok(fallbackRoot);
    assert.equal(fs.existsSync(fallbackRoot), false);
  }
);

test("exitCodeForResult preserves child process signal semantics", () => {
  assert.equal(exitCodeForResult({ status: 7 }), 7);
  assert.equal(exitCodeForResult({ status: null, signal: "SIGINT" }), 130);
  assert.equal(exitCodeForResult({ status: null, signal: "SIGTERM" }), 143);
  assert.equal(exitCodeForResult({ status: null, signal: "SIGHUP" }), 1);
});
