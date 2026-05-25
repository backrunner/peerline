#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");

const DOWNLOAD_PROGRESS_MESSAGE = "Downloading peerline binary (first run only)...";
const DOWNLOAD_PROGRESS_BAR_WIDTH = 20;
const DOWNLOAD_PROGRESS_DELAY_MS = 3_000;
const DOWNLOAD_PROGRESS_REFRESH_MS = 125;
const DOWNLOAD_PROGRESS_ANIMATION_INTERVAL_MS = 250;

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

function formatByteSize(bytes) {
  const safeBytes = Number.isFinite(bytes) && bytes > 0 ? bytes : 0;
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = safeBytes;
  let unit = 0;

  while (value >= 1024 && unit + 1 < units.length) {
    value /= 1024;
    unit += 1;
  }

  if (unit === 0) {
    return `${Math.round(value)} ${units[unit]}`;
  }

  return `${value.toFixed(1)} ${units[unit]}`;
}

function formatDownloadProgressBar(ratio, width = DOWNLOAD_PROGRESS_BAR_WIDTH) {
  const clamped = Math.max(0, Math.min(1, ratio));
  const filled = clamped >= 1 ? width : Math.floor(clamped * width);
  return `[${"#".repeat(filled)}${".".repeat(width - filled)}]`;
}

function formatIndeterminateProgressBar(frame, width = DOWNLOAD_PROGRESS_BAR_WIDTH) {
  const segmentWidth = Math.min(width, Math.max(4, Math.floor(width / 3)));
  const maxOffset = Math.max(0, width - segmentWidth);
  const offset = maxOffset === 0 ? 0 : frame % (maxOffset + 1);
  return `[${".".repeat(offset)}${"#".repeat(segmentWidth)}${".".repeat(width - segmentWidth - offset)}]`;
}

function formatDownloadStatusLine({
  downloadedBytes,
  totalBytes = null,
  elapsedMs,
  message = DOWNLOAD_PROGRESS_MESSAGE,
  progressBarWidth = DOWNLOAD_PROGRESS_BAR_WIDTH,
  nowMs = 0,
}) {
  const safeDownloadedBytes = Number.isFinite(downloadedBytes) && downloadedBytes > 0 ? downloadedBytes : 0;
  const safeElapsedMs = Number.isFinite(elapsedMs) && elapsedMs > 0 ? elapsedMs : 0;
  const speedBytesPerSecond = safeElapsedMs > 0 ? safeDownloadedBytes / (safeElapsedMs / 1000) : 0;
  const hasKnownTotal = Number.isFinite(totalBytes) && totalBytes > 0;
  const bar = hasKnownTotal
    ? formatDownloadProgressBar(safeDownloadedBytes / totalBytes, progressBarWidth)
    : formatIndeterminateProgressBar(Math.floor(nowMs / DOWNLOAD_PROGRESS_ANIMATION_INTERVAL_MS), progressBarWidth);

  if (hasKnownTotal) {
    const ratio = Math.max(0, Math.min(1, safeDownloadedBytes / totalBytes));
    return [
      message,
      bar,
      `${Math.round(ratio * 100)}%`,
      `${formatByteSize(safeDownloadedBytes)} / ${formatByteSize(totalBytes)}`,
      `${formatByteSize(speedBytesPerSecond)}/s`,
    ].join(" ");
  }

  return [
    message,
    bar,
    `${formatByteSize(safeDownloadedBytes)} downloaded`,
    `${formatByteSize(speedBytesPerSecond)}/s`,
  ].join(" ");
}

function createDownloadProgressReporter(deps = {}) {
  const stderr = deps.stderr ?? process.stderr;
  const nowImpl = deps.now ?? Date.now;
  const setTimeoutImpl = deps.setTimeout ?? setTimeout;
  const clearTimeoutImpl = deps.clearTimeout ?? clearTimeout;
  const activationDelayMs = deps.activationDelayMs ?? DOWNLOAD_PROGRESS_DELAY_MS;
  const refreshIntervalMs = deps.refreshIntervalMs ?? DOWNLOAD_PROGRESS_REFRESH_MS;
  const progressBarWidth = deps.progressBarWidth ?? DOWNLOAD_PROGRESS_BAR_WIDTH;
  const message = deps.message ?? DOWNLOAD_PROGRESS_MESSAGE;
  const isInteractive = Boolean(stderr?.isTTY);
  let activated = false;
  let downloadedBytes = 0;
  let finished = false;
  let lastLineWidth = 0;
  let lastRenderAt = Number.NEGATIVE_INFINITY;
  let plainMessageShown = false;
  let timer = null;
  let totalBytes = null;
  const startMs = nowImpl();

  const render = (force = false) => {
    if (!activated || finished) {
      return;
    }

    const nowMs = nowImpl();
    if (!force && nowMs - lastRenderAt < refreshIntervalMs) {
      return;
    }
    lastRenderAt = nowMs;

    if (!isInteractive) {
      if (!plainMessageShown) {
        stderr.write(`${message}\n`);
        plainMessageShown = true;
      }
      return;
    }

    const line = formatDownloadStatusLine({
      downloadedBytes,
      totalBytes,
      elapsedMs: nowMs - startMs,
      message,
      progressBarWidth,
      nowMs,
    });
    const width = Math.max(lastLineWidth, line.length);
    stderr.write(`\r${line.padEnd(width, " ")}`);
    lastLineWidth = width;
  };

  const activate = () => {
    if (activated || finished) {
      return;
    }
    activated = true;
    render(true);
  };

  timer = setTimeoutImpl(activate, activationDelayMs);
  if (typeof timer?.unref === "function") {
    timer.unref();
  }

  return {
    onStart(nextTotalBytes) {
      totalBytes = Number.isFinite(nextTotalBytes) && nextTotalBytes >= 0 ? nextTotalBytes : null;
      if (!activated && nowImpl() - startMs >= activationDelayMs) {
        activate();
        return;
      }
      if (activated) {
        render(true);
      }
    },
    onProgress(nextDownloadedBytes) {
      downloadedBytes = Number.isFinite(nextDownloadedBytes) && nextDownloadedBytes >= 0 ? nextDownloadedBytes : downloadedBytes;
      if (!activated && nowImpl() - startMs >= activationDelayMs) {
        activate();
        return;
      }
      render();
    },
    finish() {
      if (finished) {
        return;
      }
      finished = true;
      if (timer) {
        clearTimeoutImpl(timer);
      }
      if (activated && isInteractive && lastLineWidth > 0) {
        stderr.write(`\r${" ".repeat(lastLineWidth)}\r`);
      }
    },
  };
}

function pruneStaleCachedVersions(version = packageVersion(), env = process.env, osImpl = os, fsImpl = fs) {
  const root = cacheRoot(env, osImpl);
  let entries;

  try {
    entries = fsImpl.readdirSync(root, { withFileTypes: true });
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return;
    }
    throw error;
  }

  for (const entry of entries) {
    if (typeof entry.isDirectory !== "function" || !entry.isDirectory() || entry.name === version) {
      continue;
    }
    fsImpl.rmSync(path.join(root, entry.name), { recursive: true, force: true });
  }
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

        const totalBytesHeader = Number(response.headers["content-length"]);
        const totalBytes = Number.isFinite(totalBytesHeader) && totalBytesHeader >= 0 ? totalBytesHeader : null;
        deps.onDownloadStart?.(totalBytes);
        let downloadedBytes = 0;
        const output = fsImpl.createWriteStream(destination, { mode: 0o755 });
        output.on("error", reject);
        response.on("error", reject);
        response.on("data", (chunk) => {
          downloadedBytes += chunk.length;
          deps.onDownloadProgress?.(downloadedBytes);
        });
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
  const downloadImpl = deps.downloadToFile ?? downloadToFile;
  const version = packageVersion(fsImpl);
  const binaryPath = cachedBinaryPath(version, env, osImpl);
  const pruneCache = () => {
    try {
      pruneStaleCachedVersions(version, env, osImpl, fsImpl);
    } catch {
      // Ignore cache cleanup failures; callers care more about having a runnable binary.
    }
  };

  if (fsImpl.existsSync(binaryPath)) {
    ensureExecutable(binaryPath, fsImpl);
    pruneCache();
    return binaryPath;
  }

  const dir = path.dirname(binaryPath);
  fsImpl.mkdirSync(dir, { recursive: true });
  const tempPath = path.join(dir, `.download-${process.pid}-${Date.now()}`);
  const url = releaseAssetUrl(version, env);
  const downloadProgress = createDownloadProgressReporter({
    stderr: deps.stderr,
    now: deps.now,
    setTimeout: deps.setTimeout,
    clearTimeout: deps.clearTimeout,
  });
  const downloadDeps = {
    ...deps,
    onDownloadStart(totalBytes) {
      deps.onDownloadStart?.(totalBytes);
      downloadProgress.onStart(totalBytes);
    },
    onDownloadProgress(downloadedBytes) {
      deps.onDownloadProgress?.(downloadedBytes);
      downloadProgress.onProgress(downloadedBytes);
    },
  };

  try {
    await downloadImpl(url, tempPath, downloadDeps);
    if (process.platform !== "win32") {
      fsImpl.chmodSync(tempPath, 0o755);
    }
    fsImpl.renameSync(tempPath, binaryPath);
    ensureExecutable(binaryPath, fsImpl);
    pruneCache();
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
  } finally {
    downloadProgress.finish();
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
  process.exit(exitCodeForResult(result));
}

function exitCodeForResult(result) {
  if (Number.isInteger(result?.status)) {
    return result.status;
  }
  if (result?.signal === "SIGINT") {
    return 130;
  }
  if (result?.signal === "SIGTERM") {
    return 143;
  }
  return 1;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  DOWNLOAD_PROGRESS_MESSAGE,
  binaryName,
  cachedBinaryPath,
  cacheRoot,
  createDownloadProgressReporter,
  detectLibc,
  downloadToFile,
  downloadReleaseBinary,
  ensureExecutable,
  executeBinary,
  exitCodeForResult,
  copyBinaryToTemporaryLocation,
  formatByteSize,
  formatDownloadStatusLine,
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
