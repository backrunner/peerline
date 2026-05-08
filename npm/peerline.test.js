"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const { ensureExecutable } = require("./peerline.js");

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
