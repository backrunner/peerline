#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");

function detectLibc(reportGetter = () => process.report?.getReport?.()) {
  if (process.platform !== "linux") {
    return "";
  }

  try {
    const report = reportGetter();
    return report?.header?.glibcVersionRuntime ? "gnu" : "musl";
  } catch {
    return "gnu";
  }
}

function packageName(platform = process.platform, arch = process.arch, libc = detectLibc()) {
  if (platform === "darwin" && arch === "arm64") return "peerline-darwin-arm64";
  if (platform === "darwin" && arch === "x64") return "peerline-darwin-x64";
  if (platform === "linux" && arch === "x64") return `peerline-linux-x64-${libc === "musl" ? "musl" : "gnu"}`;
  if (platform === "linux" && arch === "arm64") return `peerline-linux-arm64-${libc === "musl" ? "musl" : "gnu"}`;
  if (platform === "win32" && arch === "x64") return "peerline-win32-x64-msvc";
  throw new Error(`unsupported platform: ${platform} ${arch}`);
}

function binaryName() {
  return process.platform === "win32" ? "peerline.exe" : "peerline";
}

function releaseAssetName(platformPackage = packageName(), binary = binaryName()) {
  return binary.endsWith(".exe") ? `${platformPackage}.exe` : platformPackage;
}

function packageVersion(fsImpl = fs) {
  const pkg = JSON.parse(fsImpl.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"));
  return pkg.version;
}

function cacheRoot(env = process.env, osImpl = os) {
  if (env.PEERLINE_BINARY_CACHE) return env.PEERLINE_BINARY_CACHE;
  if (process.platform === "win32" && env.LOCALAPPDATA) return path.join(env.LOCALAPPDATA, "peerline");
  if (process.platform === "darwin") return path.join(osImpl.homedir(), "Library", "Caches", "peerline");
  return path.join(env.XDG_CACHE_HOME || path.join(osImpl.homedir(), ".cache"), "peerline");
}

function releaseDownloadBaseUrl(version = packageVersion(), env = process.env) {
  return (env.PEERLINE_DOWNLOAD_BASE_URL || `https://github.com/backrunner/peerline/releases/download/v${version}`).replace(
    /\/+$/,
    ""
  );
}

function releaseAssetUrl(version = packageVersion(), env = process.env) {
  return `${releaseDownloadBaseUrl(version, env)}/${releaseAssetName()}`;
}

function cachedBinaryPath(version = packageVersion(), env = process.env, osImpl = os) {
  return path.join(cacheRoot(env, osImpl), version, releaseAssetName());
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

function resolvePackagedBinary(fsImpl = fs) {
  const localRelease = path.join(__dirname, "..", "target", "release", binaryName());
  if (fsImpl.existsSync(localRelease)) return localRelease;

  const localDebug = path.join(__dirname, "..", "target", "debug", binaryName());
  if (fsImpl.existsSync(localDebug)) return localDebug;

  const pkg = packageName();
  let pkgJson;
  try {
    pkgJson = require.resolve(`${pkg}/package.json`, { paths: [__dirname] });
  } catch (error) {
    if (error && error.code === "MODULE_NOT_FOUND") {
      return null;
    }
    throw error;
  }
  const candidate = path.join(path.dirname(pkgJson), "bin", binaryName());
  if (!fsImpl.existsSync(candidate)) {
    throw new Error(`missing peerline binary in ${pkg}`);
  }
  return candidate;
}

function downloadToFile(url, destination, deps = {}, redirects = 0) {
  const httpsImpl = deps.https ?? https;
  const fsImpl = deps.fs ?? fs;
  if (redirects > 5) {
    return Promise.reject(new Error(`too many redirects while downloading ${url}`));
  }

  return new Promise((resolve, reject) => {
    const request = httpsImpl.get(
      url,
      {
        headers: {
          "User-Agent": `peerline-npm/${packageVersion(fsImpl)}`,
        },
      },
      (response) => {
        if (
          response.statusCode >= 300 &&
          response.statusCode < 400 &&
          response.headers.location
        ) {
          response.resume();
          downloadToFile(new URL(response.headers.location, url).toString(), destination, deps, redirects + 1)
            .then(resolve, reject);
          return;
        }

        if (response.statusCode !== 200) {
          response.resume();
          reject(new Error(`download failed with HTTP ${response.statusCode}: ${url}`));
          return;
        }

        const output = fsImpl.createWriteStream(destination, { mode: 0o755 });
        output.on("error", reject);
        response.on("error", reject);
        output.on("finish", () => output.close(resolve));
        response.pipe(output);
      }
    );
    request.on("error", reject);
    request.setTimeout(60_000, () => {
      request.destroy(new Error(`timed out downloading ${url}`));
    });
  });
}

async function downloadReleaseBinary(deps = {}) {
  const fsImpl = deps.fs ?? fs;
  const osImpl = deps.os ?? os;
  const env = deps.env ?? process.env;
  const version = packageVersion(fsImpl);
  const binaryPath = cachedBinaryPath(version, env, osImpl);

  if (fsImpl.existsSync(binaryPath)) {
    ensureExecutable(binaryPath, fsImpl);
    return binaryPath;
  }

  const dir = path.dirname(binaryPath);
  fsImpl.mkdirSync(dir, { recursive: true });
  const tempPath = path.join(dir, `.download-${process.pid}-${Date.now()}`);
  const url = releaseAssetUrl(version, env);

  try {
    await downloadToFile(url, tempPath, deps);
    if (process.platform !== "win32") {
      fsImpl.chmodSync(tempPath, 0o755);
    }
    fsImpl.renameSync(tempPath, binaryPath);
    ensureExecutable(binaryPath, fsImpl);
    return binaryPath;
  } catch (error) {
    try {
      fsImpl.rmSync(tempPath, { force: true });
    } catch {
      // Ignore cleanup failures; the download error below is the useful one.
    }
    throw new Error(
      [
        `missing local peerline binary and could not download ${releaseAssetName()} from ${url}.`,
        error.message,
        "Install from source with cargo, or retry when the GitHub release asset is available.",
      ].join(" ")
    );
  }
}

async function resolveBinary(deps = {}) {
  return resolvePackagedBinary(deps.fs ?? fs) || downloadReleaseBinary(deps);
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

async function main(argv = process.argv.slice(2), deps = {}) {
  const result = executeBinary(await resolveBinary(deps), argv, deps);
  process.exit(result.status ?? 1);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  binaryName,
  cachedBinaryPath,
  cacheRoot,
  detectLibc,
  downloadReleaseBinary,
  ensureExecutable,
  executeBinary,
  copyBinaryToTemporaryLocation,
  formatExecutionFallbackError,
  isRetryableExecutionError,
  main,
  packageName,
  packageVersion,
  releaseAssetName,
  releaseAssetUrl,
  releaseDownloadBaseUrl,
  resolvePackagedBinary,
  resolveBinary,
};
