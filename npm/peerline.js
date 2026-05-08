#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

function packageName() {
  const platform = process.platform;
  const arch = process.arch;
  if (platform === "darwin" && arch === "arm64") return "@peerline/peerline-darwin-arm64";
  if (platform === "darwin" && arch === "x64") return "@peerline/peerline-darwin-x64";
  if (platform === "linux" && arch === "x64") return "@peerline/peerline-linux-x64-gnu";
  if (platform === "linux" && arch === "arm64") return "@peerline/peerline-linux-arm64-musl";
  if (platform === "win32" && arch === "x64") return "@peerline/peerline-win32-x64-msvc";
  throw new Error(`unsupported platform: ${platform} ${arch}`);
}

function binaryName() {
  return process.platform === "win32" ? "peerline.exe" : "peerline";
}

function ensureExecutable(binaryPath) {
  if (process.platform === "win32") {
    return;
  }

  try {
    fs.accessSync(binaryPath, fs.constants.X_OK);
    return;
  } catch {
    // Fall through and repair the mode bits below.
  }

  const stat = fs.statSync(binaryPath);
  fs.chmodSync(binaryPath, stat.mode | 0o111);
}

function resolveBinary() {
  const bundledBinary = path.join(__dirname, "bin", binaryName());
  if (fs.existsSync(bundledBinary)) return bundledBinary;

  const localRelease = path.join(__dirname, "..", "target", "release", binaryName());
  if (fs.existsSync(localRelease)) return localRelease;

  const localDebug = path.join(__dirname, "..", "target", "debug", binaryName());
  if (fs.existsSync(localDebug)) return localDebug;

  const pkg = packageName();
  const pkgJson = require.resolve(`${pkg}/package.json`, { paths: [__dirname] });
  const candidate = path.join(path.dirname(pkgJson), "bin", binaryName());
  if (!fs.existsSync(candidate)) {
    throw new Error(`missing peerline binary in ${pkg}`);
  }
  return candidate;
}

function main(argv = process.argv.slice(2)) {
  const binary = resolveBinary();
  ensureExecutable(binary);

  const result = spawnSync(binary, argv, {
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  process.exit(result.status ?? 1);
}

if (require.main === module) {
  main();
}

module.exports = {
  binaryName,
  ensureExecutable,
  main,
  packageName,
  resolveBinary,
};
