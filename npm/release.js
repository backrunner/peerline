#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { detectLibc, packageName } = require("./peerline.js");

const repoRoot = path.resolve(__dirname, "..");
const repository = {
  type: "git",
  url: "https://github.com/backrunner/peerline",
};
const packageJsonPath = path.join(repoRoot, "package.json");
const cargoTomlPath = path.join(repoRoot, "Cargo.toml");
const cargoLockPath = path.join(repoRoot, "Cargo.lock");
const workspacePackageNames = [
  "peerline-cli",
  "peerline-core",
  "peerline-crypto",
  "peerline-net",
  "peerline-testkit",
  "peerline-transfer",
  "peerline-tui",
];
const platformPackageSpecs = {
  "peerline-darwin-arm64": {
    arch: "arm64",
    platform: "darwin",
  },
  "peerline-darwin-x64": {
    arch: "x64",
    platform: "darwin",
  },
  "peerline-linux-arm64-gnu": {
    arch: "arm64",
    libc: "gnu",
    libcField: ["glibc"],
    platform: "linux",
  },
  "peerline-linux-arm64-musl": {
    arch: "arm64",
    cargoTarget: "aarch64-unknown-linux-musl",
    libc: "musl",
    libcField: ["musl"],
    platform: "linux",
  },
  "peerline-linux-x64-gnu": {
    arch: "x64",
    libc: "gnu",
    libcField: ["glibc"],
    platform: "linux",
  },
  "peerline-linux-x64-musl": {
    arch: "x64",
    cargoTarget: "x86_64-unknown-linux-musl",
    libc: "musl",
    libcField: ["musl"],
    platform: "linux",
  },
  "peerline-win32-x64-msvc": {
    arch: "x64",
    platform: "win32",
  },
};
const platformPackageNames = Object.keys(platformPackageSpecs).sort();

function usage() {
  return [
    "Usage: npm run release:<alpha|beta|stable> -- [options]",
    "",
    "Options:",
    "  --channel <channel>   Release channel: alpha, beta, stable, or release.",
    "  --version <version>   Release an explicit version, e.g. 0.1.0-alpha.2.",
    "  --current             Publish the current version without bumping files.",
    "  --otp <code>          npm two-factor one-time password.",
    "  --tag <tag>           npm dist-tag. Defaults to alpha, beta, or latest.",
    "  --access <access>     npm package access. Defaults to public.",
    "  --platform-package <name>",
    "                        Build/publish a specific platform package.",
    "  --main-only           Publish only the main JS shim package.",
    "  --platform-only       Publish only the current platform binary package.",
    "  --ignore-existing     Treat already-published package versions as success.",
    "  --skip-tests          Skip npm run lint and npm test before publishing.",
    "  --no-publish          Bump, verify, and optionally commit without npm publish.",
    "  --no-commit           Do not create the release bump commit.",
    "  --allow-dirty         Allow starting from a dirty git worktree.",
    "  --help                Show this help.",
  ].join("\n");
}

function parseArgs(argv) {
  const options = {
    access: "public",
    allowDirty: false,
    channel: process.env.PEERLINE_RELEASE_CHANNEL || "alpha",
    commit: true,
    current: false,
    otp: process.env.NPM_CONFIG_OTP || "",
    publish: true,
    publishTarget: "both",
    platformPackage: process.env.PEERLINE_PLATFORM_PACKAGE || "",
    ignoreExisting: false,
    skipTests: false,
    tag: "",
    version: "",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const readValue = (name) => {
      const inlinePrefix = `${name}=`;
      if (arg.startsWith(inlinePrefix)) {
        return arg.slice(inlinePrefix.length);
      }
      const next = argv[index + 1];
      if (!next || next.startsWith("--")) {
        throw new Error(`${name} requires a value`);
      }
      index += 1;
      return next;
    };

    if (arg === "--help" || arg === "-h") {
      options.help = true;
    } else if (arg === "--channel" || arg.startsWith("--channel=")) {
      options.channel = readValue("--channel");
    } else if (arg === "--version" || arg.startsWith("--version=")) {
      options.version = readValue("--version");
    } else if (arg === "--otp" || arg.startsWith("--otp=")) {
      options.otp = readValue("--otp");
    } else if (arg === "--tag" || arg.startsWith("--tag=")) {
      options.tag = readValue("--tag");
    } else if (arg === "--access" || arg.startsWith("--access=")) {
      options.access = readValue("--access");
    } else if (arg === "--platform-package" || arg.startsWith("--platform-package=")) {
      options.platformPackage = readValue("--platform-package");
    } else if (arg === "--main-only") {
      options.publishTarget = "main";
    } else if (arg === "--platform-only") {
      options.publishTarget = "platform";
    } else if (arg === "--ignore-existing") {
      options.ignoreExisting = true;
    } else if (arg === "--current") {
      options.current = true;
    } else if (arg === "--skip-tests") {
      options.skipTests = true;
    } else if (arg === "--no-publish") {
      options.publish = false;
    } else if (arg === "--no-commit") {
      options.commit = false;
    } else if (arg === "--allow-dirty") {
      options.allowDirty = true;
    } else {
      throw new Error(`unknown option: ${arg}`);
    }
  }

  if (options.current && options.version) {
    throw new Error("--current and --version cannot be used together");
  }

  options.channel = normalizeChannel(options.channel);
  if (!options.tag) {
    options.tag = distTagForChannel(options.channel);
  }
  if (options.platformPackage && !platformPackageSpecs[options.platformPackage]) {
    throw new Error(`unsupported platform package: ${options.platformPackage}`);
  }

  return options;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function cargoWorkspaceVersion(cargoToml) {
  const match = cargoToml.match(/^\[workspace\.package\][\s\S]*?^version\s*=\s*"([^"]+)"/m);
  if (!match) {
    throw new Error("could not find [workspace.package] version in Cargo.toml");
  }
  return match[1];
}

function setCargoWorkspaceVersion(cargoToml, version) {
  return cargoToml.replace(
    /^(\[workspace\.package\][\s\S]*?^version\s*=\s*")[^"]+(")/m,
    `$1${version}$2`
  );
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function setCargoLockVersion(cargoLock, version) {
  let updated = cargoLock;

  for (const name of workspacePackageNames) {
    const pattern = new RegExp(
      `(\\[\\[package\\]\\]\\nname = "${escapeRegExp(name)}"\\n[\\s\\S]*?^version = ")([^"]+)(")`,
      "m"
    );
    if (!pattern.test(updated)) {
      throw new Error(`could not find ${name} in Cargo.lock`);
    }
    updated = updated.replace(pattern, `$1${version}$3`);
  }

  return updated;
}

function normalizeChannel(channel) {
  if (channel === "release") {
    return "stable";
  }
  if (channel === "alpha" || channel === "beta" || channel === "stable") {
    return channel;
  }
  throw new Error(`unsupported release channel: ${channel}`);
}

function distTagForChannel(channel) {
  const normalized = normalizeChannel(channel);
  if (normalized === "stable") {
    return "latest";
  }
  return normalized;
}

function parseVersion(version) {
  const match = version.match(/^(\d+)\.(\d+)\.(\d+)(?:-([a-z]+)\.(\d+))?$/);
  if (!match) {
    throw new Error(`invalid release version: ${version}`);
  }
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4] || "",
    prereleaseNumber: match[5] === undefined ? null : Number(match[5]),
  };
}

function baseVersion(version) {
  const parsed = parseVersion(version);
  return `${parsed.major}.${parsed.minor}.${parsed.patch}`;
}

function nextReleaseVersion(version, channel) {
  const normalized = normalizeChannel(channel);
  const parsed = parseVersion(version);
  const base = `${parsed.major}.${parsed.minor}.${parsed.patch}`;

  if (normalized === "alpha") {
    if (parsed.prerelease === "alpha") {
      return `${base}-alpha.${parsed.prereleaseNumber + 1}`;
    }
    if (parsed.prerelease) {
      return `${base}-alpha.0`;
    }
    return `${parsed.major}.${parsed.minor}.${parsed.patch + 1}-alpha.0`;
  }

  if (normalized === "beta") {
    if (parsed.prerelease === "beta") {
      return `${base}-beta.${parsed.prereleaseNumber + 1}`;
    }
    if (parsed.prerelease) {
      return `${base}-beta.0`;
    }
    return `${parsed.major}.${parsed.minor}.${parsed.patch + 1}-beta.0`;
  }

  if (parsed.prerelease) {
    return base;
  }
  return `${parsed.major}.${parsed.minor}.${parsed.patch + 1}`;
}

function nextAlphaVersion(version) {
  return nextReleaseVersion(version, "alpha");
}

function assertReleaseVersion(version, channel) {
  const normalized = normalizeChannel(channel);
  const parsed = parseVersion(version);

  if (normalized === "stable") {
    if (parsed.prerelease) {
      throw new Error(`stable release version expected, got: ${version}`);
    }
    return;
  }

  if (parsed.prerelease !== normalized || parsed.prereleaseNumber === null) {
    throw new Error(`${normalized} release version expected, got: ${version}`);
  }
}

function assertAlphaVersion(version) {
  assertReleaseVersion(version, "alpha");
}

function setPlatformDependencyVersions(packageJson, version) {
  const optionalDependencies = { ...(packageJson.optionalDependencies || {}) };

  for (const name of platformPackageNames) {
    delete optionalDependencies[name];
  }

  const next = {
    ...packageJson,
  };
  if (Object.keys(optionalDependencies).length > 0) {
    next.optionalDependencies = optionalDependencies;
  } else {
    delete next.optionalDependencies;
  }
  return next;
}

function run(command, args, options = {}) {
  console.log(`$ ${[command, ...args].join(" ")}`);
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: options.capture ? "pipe" : "inherit",
    env: {
      ...process.env,
      ...(options.env || {}),
    },
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    const stderr = result.stderr ? `\n${result.stderr.trim()}` : "";
    throw new Error(`${command} exited with ${result.status}${stderr}`);
  }
  return result.stdout || "";
}

function gitStatus() {
  return run("git", ["status", "--porcelain"], { capture: true }).trim();
}

function assertCleanWorktree() {
  const status = gitStatus();
  if (status) {
    throw new Error(`git worktree is dirty; commit or stash first:\n${status}`);
  }
}

function currentVersions() {
  const packageJson = readJson(packageJsonPath);
  const cargoToml = fs.readFileSync(cargoTomlPath, "utf8");
  const cargoVersion = cargoWorkspaceVersion(cargoToml);
  if (packageJson.version !== cargoVersion) {
    throw new Error(`version mismatch: package.json=${packageJson.version}, Cargo.toml=${cargoVersion}`);
  }
  return {
    cargoToml,
    packageJson,
    version: packageJson.version,
  };
}

function setProjectVersion(version, versions = currentVersions()) {
  const nextPackageJson = setPlatformDependencyVersions(
    {
      ...versions.packageJson,
      version,
    },
    version
  );

  writeJson(packageJsonPath, nextPackageJson);
  fs.writeFileSync(cargoTomlPath, setCargoWorkspaceVersion(versions.cargoToml, version));
  fs.writeFileSync(cargoLockPath, setCargoLockVersion(fs.readFileSync(cargoLockPath, "utf8"), version));
  run("cargo", ["check", "--workspace", "--all-targets", "--locked"]);
}

function verifyCargoLockVersion(version) {
  const lock = fs.readFileSync(cargoLockPath, "utf8");
  const packageBlocks = lock.split(/\n(?=\[\[package\]\]\n)/);
  const stalePeerlinePackages = packageBlocks
    .filter((block) => /^name = "peerline-/m.test(block))
    .filter((block) => !new RegExp(`^version = "${version.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`, "m").test(block))
    .map((block) => block.match(/^name = "([^"]+)"/m)?.[1])
    .filter(Boolean);

  if (stalePeerlinePackages.length > 0) {
    throw new Error(`Cargo.lock was not updated for: ${stalePeerlinePackages.join(", ")}`);
  }
}

function releaseName(channel) {
  return normalizeChannel(channel) === "stable" ? "release" : normalizeChannel(channel);
}

function commitRelease(version, channel) {
  run("git", ["add", "package.json", "Cargo.toml", "Cargo.lock"]);
  const staged = run("git", ["diff", "--cached", "--name-only"], { capture: true }).trim();
  if (!staged) {
    console.log("No version changes to commit.");
    return;
  }

  run("git", [
    "-c",
    "user.name=BackRunner",
    "-c",
    "user.email=dev@backrunner.top",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "-m",
    `chore: bump ${releaseName(channel)} release to ${version}`,
  ]);
}

function platformBinaryName(platform) {
  return platform === "win32" ? "peerline.exe" : "peerline";
}

function platformPackageInfo(name, version) {
  const spec = platformPackageSpecs[name];
  if (!spec) {
    throw new Error(`unsupported platform package: ${name}`);
  }

  return {
    ...spec,
    binaryName: platformBinaryName(spec.platform),
    name,
    version,
  };
}

function currentPlatformPackage(version) {
  const platform = process.platform;
  const arch = process.arch;
  const libc = detectLibc();
  const name = packageName(platform, arch, libc);
  return platformPackageInfo(name, version);
}

function selectedPlatformPackage(version, options) {
  if (options.platformPackage) {
    return platformPackageInfo(options.platformPackage, version);
  }
  return currentPlatformPackage(version);
}

function preparePlatformPackage(version, options = {}) {
  const info = selectedPlatformPackage(version, options);
  const buildArgs = ["build", "--release", "-p", "peerline-cli"];
  if (info.cargoTarget) {
    buildArgs.push("--target", info.cargoTarget);
  }
  run("cargo", buildArgs);

  const packageDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-platform-package-"));
  const binDir = path.join(packageDir, "bin");
  const releaseDir = info.cargoTarget
    ? path.join(repoRoot, "target", info.cargoTarget, "release")
    : path.join(repoRoot, "target", "release");
  const source = path.join(releaseDir, info.binaryName);
  const destination = path.join(binDir, info.binaryName);

  if (!fs.existsSync(source)) {
    throw new Error(`missing release binary: ${source}`);
  }

  fs.mkdirSync(binDir, { recursive: true });
  fs.copyFileSync(source, destination);
  if (process.platform !== "win32") {
    fs.chmodSync(destination, 0o755);
  }

  const packageJson = {
    name: info.name,
    version,
    description: `Peerline binary for ${info.platform}-${info.arch}${info.platform === "linux" ? `-${info.libc}` : ""}`,
    license: "Apache-2.0",
    repository,
    os: [info.platform],
    cpu: [info.arch],
    files: ["bin"],
  };

  if (info.libcField) {
    packageJson.libc = info.libcField;
  }

  writeJson(path.join(packageDir, "package.json"), packageJson);

  return {
    ...info,
    packageDir,
    cleanup() {
      fs.rmSync(packageDir, { recursive: true, force: true });
    },
  };
}

function publishMainPackage(version, options) {
  if (options.ignoreExisting && npmPackageExists("peerline", version)) {
    console.log(`peerline@${version} already exists; skipping publish.`);
    return;
  }

  const args = ["publish", "--tag", options.tag, "--access", options.access];
  if (options.otp) {
    args.push(`--otp=${options.otp}`);
  }
  run("npm", args);
}

function npmPackageExists(name, version) {
  const result = spawnSync("npm", ["view", `${name}@${version}`, "version"], {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: "pipe",
  });

  return result.status === 0 && result.stdout.trim() === version;
}

function publishPlatformPackage(platformPackage, options) {
  const packArgs = ["pack", "--dry-run", "--json", platformPackage.packageDir];
  run("npm", packArgs);

  if (!options.publish) {
    console.log(`Skipping npm publish for ${platformPackage.name}@${platformPackage.version}.`);
    return;
  }
  if (options.ignoreExisting && npmPackageExists(platformPackage.name, platformPackage.version)) {
    console.log(`${platformPackage.name}@${platformPackage.version} already exists; skipping publish.`);
    return;
  }

  const publishArgs = ["publish", platformPackage.packageDir, "--tag", options.tag, "--access", options.access];
  if (options.otp) {
    publishArgs.push(`--otp=${options.otp}`);
  }
  run("npm", publishArgs);
}

function targetIncludes(options, target) {
  return options.publishTarget === "both" || options.publishTarget === target;
}

function release(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(usage());
    return;
  }

  if (!options.allowDirty) {
    assertCleanWorktree();
  }

  const versions = currentVersions();
  const targetVersion = options.current
    ? versions.version
    : options.version || nextReleaseVersion(versions.version, options.channel);
  assertReleaseVersion(targetVersion, options.channel);

  if (targetVersion !== versions.version) {
    console.log(`Bumping ${versions.version} -> ${targetVersion}`);
    setProjectVersion(targetVersion, versions);
  } else {
    console.log(`Using current version ${targetVersion}`);
  }

  verifyCargoLockVersion(targetVersion);

  if (!options.skipTests) {
    run("npm", ["run", "lint"]);
    run("npm", ["test"]);
  }

  let platformPackage = null;
  try {
    if (targetIncludes(options, "platform")) {
      platformPackage = preparePlatformPackage(targetVersion, options);
      publishPlatformPackage(platformPackage, { ...options, publish: false });
    }

    if (targetIncludes(options, "main")) {
      run("npm", ["pack", "--dry-run", "--json"]);
      run("node", ["-e", "const shim = require('./npm/peerline.js'); console.log(shim.packageName());"]);
    }

    if (options.commit) {
      commitRelease(targetVersion, options.channel);
    }

    if (targetIncludes(options, "platform") && platformPackage) {
      publishPlatformPackage(platformPackage, options);
    }

    if (targetIncludes(options, "main")) {
      if (options.publish) {
        publishMainPackage(targetVersion, options);
      } else {
        console.log(`Skipping npm publish for peerline@${targetVersion}.`);
      }
    }
  } finally {
    if (platformPackage) {
      platformPackage.cleanup();
    }
  }
}

if (require.main === module) {
  try {
    release();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = {
  assertAlphaVersion,
  assertReleaseVersion,
  cargoWorkspaceVersion,
  distTagForChannel,
  nextReleaseVersion,
  nextAlphaVersion,
  normalizeChannel,
  parseVersion,
  parseArgs,
  repository,
  platformPackageInfo,
  platformPackageNames,
  platformPackageSpecs,
  release,
  npmPackageExists,
  setCargoWorkspaceVersion,
  setCargoLockVersion,
  setPlatformDependencyVersions,
  workspacePackageNames,
};
