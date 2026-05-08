#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

function binaryName() {
  return process.platform === "win32" ? "peerline.exe" : "peerline";
}

function buildReleaseBinary() {
  const result = spawnSync("cargo", ["build", "--release", "-p", "peerline-cli"], {
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`cargo build failed with exit code ${result.status ?? 1}`);
  }
}

function copyBundledBinary() {
  const source = path.join(__dirname, "..", "target", "release", binaryName());
  const destination = path.join(__dirname, "bin", binaryName());

  if (!fs.existsSync(source)) {
    throw new Error(`missing release binary: ${source}`);
  }

  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.copyFileSync(source, destination);
  fs.chmodSync(destination, 0o755);
}

buildReleaseBinary();
copyBundledBinary();
