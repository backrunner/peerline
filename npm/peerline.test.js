"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { PassThrough, Readable } = require("node:stream");
const test = require("node:test");

const {
  DOWNLOAD_PROGRESS_MESSAGE,
  createDownloadProgressReporter,
  downloadToFile,
  downloadReleaseBinary,
  cachedBinaryPath,
  detectLibc,
  ensureExecutable,
  executeBinary,
  exitCodeForResult,
  formatByteSize,
  formatDownloadStatusLine,
  packageName,
  packageVersion,
  releaseAssetName,
  releaseAssetUrl,
} = require("./peerline.js");

class FakeClock {
  constructor(startMs = 0) {
    this.nextTimerId = 1;
    this.nowMs = startMs;
    this.timers = [];
  }

  now = () => this.nowMs;

  setTimeout = (fn, delay) => {
    const timer = {
      at: this.nowMs + delay,
      cleared: false,
      id: this.nextTimerId,
      unref() {
        return this;
      },
    };
    this.nextTimerId += 1;
    timer.fn = fn;
    this.timers.push(timer);
    return timer;
  };

  clearTimeout = (timer) => {
    if (timer) {
      timer.cleared = true;
    }
  };

  advance(ms) {
    const target = this.nowMs + ms;

    while (true) {
      let nextTimer = null;
      for (const timer of this.timers) {
        if (timer.cleared || timer.at > target) {
          continue;
        }
        if (!nextTimer || timer.at < nextTimer.at || (timer.at === nextTimer.at && timer.id < nextTimer.id)) {
          nextTimer = timer;
        }
      }

      if (!nextTimer) {
        break;
      }

      this.nowMs = nextTimer.at;
      nextTimer.cleared = true;
      nextTimer.fn();
    }

    this.nowMs = target;
  }
}

class FakeStderr {
  constructor(isTTY) {
    this.isTTY = isTTY;
    this.writes = [];
  }

  write(chunk) {
    this.writes.push(chunk);
    return true;
  }

  text() {
    return this.writes.join("");
  }
}

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

test("formatByteSize uses IEC units for the download progress UI", () => {
  assert.equal(formatByteSize(0), "0 B");
  assert.equal(formatByteSize(999), "999 B");
  assert.equal(formatByteSize(1536), "1.5 KiB");
  assert.equal(formatByteSize(4 * 1024 * 1024), "4.0 MiB");
});

test("formatDownloadStatusLine renders a fixed-width ASCII progress bar with speed", () => {
  const line = formatDownloadStatusLine({
    downloadedBytes: 20 * 1024 * 1024,
    elapsedMs: 5_000,
    nowMs: 5_000,
    totalBytes: 40 * 1024 * 1024,
  });

  assert.equal(
    line,
    `${DOWNLOAD_PROGRESS_MESSAGE} [##########..........] 50% 20.0 MiB / 40.0 MiB 4.0 MiB/s`
  );
});

test("createDownloadProgressReporter waits three seconds before rendering to an interactive terminal", () => {
  const clock = new FakeClock();
  const stderr = new FakeStderr(true);
  const reporter = createDownloadProgressReporter({
    clearTimeout: clock.clearTimeout,
    now: clock.now,
    setTimeout: clock.setTimeout,
    stderr,
  });

  reporter.onStart(100);
  reporter.onProgress(25);
  clock.advance(2_999);
  assert.equal(stderr.writes.length, 0);

  clock.advance(1);
  assert.equal(stderr.writes.length, 1);
  assert.match(stderr.writes[0], /^\rDownloading peerline binary \(first run only\)\.\.\. \[[#.]{20}\] 25% 25 B \/ 100 B 8 B\/s/);

  reporter.finish();
  assert.match(stderr.writes.at(-1), /^\r +\r$/);
});

test("createDownloadProgressReporter emits a single static message on non-interactive stderr", () => {
  const clock = new FakeClock();
  const stderr = new FakeStderr(false);
  const reporter = createDownloadProgressReporter({
    clearTimeout: clock.clearTimeout,
    now: clock.now,
    setTimeout: clock.setTimeout,
    stderr,
  });

  reporter.onStart(100);
  reporter.onProgress(25);
  clock.advance(3_000);

  assert.equal(stderr.text(), `${DOWNLOAD_PROGRESS_MESSAGE}\n`);

  clock.advance(500);
  reporter.onProgress(50);
  reporter.finish();
  assert.equal(stderr.text(), `${DOWNLOAD_PROGRESS_MESSAGE}\n`);
});

test("downloadToFile reports progress only for the final redirect target", async () => {
  const requests = [];
  const startedTotals = [];
  const progressUpdates = [];
  const writes = new Map();
  const fakeFs = {
    createWriteStream(destination) {
      const output = new PassThrough();
      const chunks = [];
      output.on("data", (chunk) => chunks.push(chunk));
      output.close = (callback) => {
        writes.set(destination, Buffer.concat(chunks).toString("utf8"));
        callback?.();
      };
      return output;
    },
    readFileSync: fs.readFileSync.bind(fs),
  };
  const responses = [
    Object.assign(Readable.from([]), {
      headers: { location: "/download/final" },
      statusCode: 302,
    }),
    Object.assign(Readable.from([Buffer.from("abc"), Buffer.from("def")]), {
      headers: { "content-length": "6" },
      statusCode: 200,
    }),
  ];
  const fakeHttps = {
    get(url, options, callback) {
      requests.push({ options, url });
      const request = new EventEmitter();
      request.setTimeout = () => {};
      process.nextTick(() => callback(responses.shift()));
      return request;
    },
  };

  await downloadToFile("https://example.invalid/download/start", "/tmp/peerline-download", {
    fs: fakeFs,
    https: fakeHttps,
    onDownloadProgress(downloadedBytes) {
      progressUpdates.push(downloadedBytes);
    },
    onDownloadStart(totalBytes) {
      startedTotals.push(totalBytes);
    },
  });

  assert.deepEqual(
    requests.map(({ url }) => url),
    ["https://example.invalid/download/start", "https://example.invalid/download/final"]
  );
  assert.deepEqual(startedTotals, [6]);
  assert.deepEqual(progressUpdates, [3, 6]);
  assert.equal(writes.get("/tmp/peerline-download"), "abcdef");
});

test("downloadReleaseBinary stays quiet when the download finishes before the three-second delay", async (t) => {
  const clock = new FakeClock();
  const stderr = new FakeStderr(true);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const env = { PEERLINE_BINARY_CACHE: tempDir };

  await downloadReleaseBinary({
    clearTimeout: clock.clearTimeout,
    env,
    fs,
    now: clock.now,
    os,
    setTimeout: clock.setTimeout,
    stderr,
    async downloadToFile(url, destination, deps) {
      assert.equal(url, releaseAssetUrl(packageVersion(), env));
      deps.onDownloadStart?.(100);
      deps.onDownloadProgress?.(100);
      fs.writeFileSync(destination, "downloaded");
    },
  });

  assert.equal(stderr.writes.length, 0);
});

test("downloadReleaseBinary renders and clears a progress line after a slow download", async (t) => {
  const clock = new FakeClock();
  const stderr = new FakeStderr(true);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const env = { PEERLINE_BINARY_CACHE: tempDir };

  const resolved = await downloadReleaseBinary({
    clearTimeout: clock.clearTimeout,
    env,
    fs,
    now: clock.now,
    os,
    setTimeout: clock.setTimeout,
    stderr,
    async downloadToFile(url, destination, deps) {
      assert.equal(url, releaseAssetUrl(packageVersion(), env));
      deps.onDownloadStart?.(100);
      deps.onDownloadProgress?.(25);
      clock.advance(3_000);
      clock.advance(125);
      deps.onDownloadProgress?.(50);
      clock.advance(125);
      deps.onDownloadProgress?.(100);
      fs.writeFileSync(destination, "downloaded");
    },
  });

  assert.equal(fs.existsSync(resolved), true);
  assert.match(stderr.text(), /\rDownloading peerline binary \(first run only\)\.\.\./);
  assert.match(stderr.writes.at(-1), /^\r +\r$/);
});

test("downloadReleaseBinary clears the progress line before surfacing download errors", async (t) => {
  const clock = new FakeClock();
  const stderr = new FakeStderr(true);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-npm-"));
  t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

  const env = { PEERLINE_BINARY_CACHE: tempDir };

  await assert.rejects(
    () =>
      downloadReleaseBinary({
        clearTimeout: clock.clearTimeout,
        env,
        fs,
        now: clock.now,
        os,
        setTimeout: clock.setTimeout,
        stderr,
        async downloadToFile(url, destination, deps) {
          assert.equal(url, releaseAssetUrl(packageVersion(), env));
          deps.onDownloadStart?.(100);
          deps.onDownloadProgress?.(50);
          clock.advance(3_000);
          fs.writeFileSync(destination, "partial");
          throw new Error("network blew up");
        },
      }),
    /network blew up/
  );

  assert.match(stderr.text(), /\rDownloading peerline binary \(first run only\)\.\.\./);
  assert.match(stderr.writes.at(-1), /^\r +\r$/);
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
