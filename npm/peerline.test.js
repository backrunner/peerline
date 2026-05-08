"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const { ensureExecutable, executeBinary } = require("./peerline.js");

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
