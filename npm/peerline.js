#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
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

function ensureExecutable(binaryPath, fsImpl = fs) {
  if (process.platform === "win32") {
    return;
  }

  try {
    fsImpl.accessSync(binaryPath, fs.constants.X_OK);
    return;
  } catch {
    // Fall through and repair the mode bits below.
  }

  try {
    const stat = fsImpl.statSync(binaryPath);
    fsImpl.chmodSync(binaryPath, stat.mode | 0o111);
  } catch (error) {
    if (error && (error.code === "EACCES" || error.code === "EPERM" || error.code === "EROFS")) {
      return;
    }
    throw error;
  }
}

function isRetryableExecutionError(error) {
  return Boolean(error && (error.code === "EACCES" || error.code === "ENOEXEC"));
}

function copyBinaryToTemporaryLocation(binaryPath, fsImpl = fs, osImpl = os) {
  const tempDir = fsImpl.mkdtempSync(path.join(osImpl.tmpdir(), "peerline-exec-"));
  const tempBinary = path.join(tempDir, binaryName());

  fsImpl.copyFileSync(binaryPath, tempBinary);
  fsImpl.chmodSync(tempBinary, 0o755);

  return {
    binaryPath: tempBinary,
    cleanup() {
      fsImpl.rmSync(tempDir, { recursive: true, force: true });
    },
  };
}

function formatExecutionFallbackError(binaryPath, primaryCode, fallbackCode, fallbackPath) {
  return [
    `peerline could not execute its bundled binary from ${binaryPath}.`,
    `Primary error: ${primaryCode}.`,
    fallbackPath
      ? `Temporary copy at ${fallbackPath} also failed with ${fallbackCode}.`
      : `Creating a temporary executable copy also failed with ${fallbackCode}.`,
    "This usually means the package cache or temporary directory cannot execute files.",
  ].join(" ");
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

function executeBinary(binaryPath, argv = process.argv.slice(2), deps = {}) {
  const spawnImpl = deps.spawnSync ?? spawnSync;
  const fsImpl = deps.fs ?? fs;
  const osImpl = deps.os ?? os;
  const spawnOptions = {
    stdio: "inherit",
    ...(deps.spawnOptions ?? {}),
  };

  ensureExecutable(binaryPath, fsImpl);

  const primaryResult = spawnImpl(binaryPath, argv, spawnOptions);
  if (!primaryResult.error) {
    return primaryResult;
  }
  if (!isRetryableExecutionError(primaryResult.error)) {
    throw primaryResult.error;
  }

  let tempBinary;
  try {
    try {
      tempBinary = copyBinaryToTemporaryLocation(binaryPath, fsImpl, osImpl);
    } catch (copyError) {
      const error = new Error(
        formatExecutionFallbackError(binaryPath, primaryResult.error.code ?? "unknown", copyError.code ?? "unknown")
      );
      error.cause = copyError;
      throw error;
    }

    const retryResult = spawnImpl(tempBinary.binaryPath, argv, spawnOptions);
    if (!retryResult.error) {
      return retryResult;
    }

    const error = new Error(
      formatExecutionFallbackError(
        binaryPath,
        primaryResult.error.code ?? "unknown",
        retryResult.error.code ?? "unknown",
        tempBinary.binaryPath
      )
    );
    error.cause = retryResult.error;
    throw error;
  } finally {
    if (tempBinary) {
      try {
        tempBinary.cleanup();
      } catch {
        // Ignore cleanup failures; execution has already finished or failed.
      }
    }
  }
}

function main(argv = process.argv.slice(2), deps = {}) {
  const result = executeBinary(resolveBinary(), argv, deps);
  process.exit(result.status ?? 1);
}

if (require.main === module) {
  main();
}

module.exports = {
  binaryName,
  ensureExecutable,
  executeBinary,
  copyBinaryToTemporaryLocation,
  formatExecutionFallbackError,
  isRetryableExecutionError,
  main,
  packageName,
  resolveBinary,
};
