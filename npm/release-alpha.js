#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { binaryName, detectLibc, packageName } = require("./peerline.js");

const repoRoot = path.resolve(__dirname, "..");
const packageJsonPath = path.join(repoRoot, "package.json");
const cargoTomlPath = path.join(repoRoot, "Cargo.toml");
const cargoLockPath = path.join(repoRoot, "Cargo.lock");
const platformPackageNames = [
  "peerline-darwin-arm64",
  "peerline-darwin-x64",
  "peerline-linux-arm64-gnu",
  "peerline-linux-arm64-musl",
  "peerline-linux-x64-gnu",
  "peerline-linux-x64-musl",
  "peerline-win32-x64-msvc",
];

function usage() {
  return [
    "Usage: npm run release:alpha -- [options]",
    "",
    "Options:",
    "  --version <version>   Release an explicit alpha version, e.g. 0.1.0-alpha.2.",
    "  --current             Publish the current version without bumping files.",
    "  --otp <code>          npm two-factor one-time password.",
    "  --tag <tag>           npm dist-tag. Defaults to alpha.",
    "  --access <access>     npm package access. Defaults to public.",
    "  --main-only           Publish only the main JS shim package.",
    "  --platform-only       Publish only the current platform binary package.",
    "  --skip-tests          Skip npm test before publishing.",
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
    commit: true,
    current: false,
    otp: process.env.NPM_CONFIG_OTP || "",
    publish: true,
    publishTarget: "both",
    skipTests: false,
    tag: "alpha",
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
    } else if (arg === "--version" || arg.startsWith("--version=")) {
      options.version = readValue("--version");
    } else if (arg === "--otp" || arg.startsWith("--otp=")) {
      options.otp = readValue("--otp");
    } else if (arg === "--tag" || arg.startsWith("--tag=")) {
      options.tag = readValue("--tag");
    } else if (arg === "--access" || arg.startsWith("--access=")) {
      options.access = readValue("--access");
    } else if (arg === "--main-only") {
      options.publishTarget = "main";
    } else if (arg === "--platform-only") {
      options.publishTarget = "platform";
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

function nextAlphaVersion(version) {
  const match = version.match(/^(\d+)\.(\d+)\.(\d+)-alpha\.(\d+)$/);
  if (!match) {
    throw new Error(`current version is not an alpha release: ${version}`);
  }
  return `${match[1]}.${match[2]}.${match[3]}-alpha.${Number(match[4]) + 1}`;
}

function assertAlphaVersion(version) {
  if (!/^\d+\.\d+\.\d+-alpha\.\d+$/.test(version)) {
    throw new Error(`alpha release version expected, got: ${version}`);
  }
}

function setPlatformDependencyVersions(packageJson, version) {
  const optionalDependencies = {
    ...(packageJson.optionalDependencies || {}),
  };

  for (const name of platformPackageNames) {
    optionalDependencies[name] = version;
  }

  return {
    ...packageJson,
    optionalDependencies,
  };
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
  run("cargo", ["metadata", "--format-version=1", "--no-deps"], { capture: true });
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

function commitRelease(version) {
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
    `chore: bump alpha release to ${version}`,
  ]);
}

function currentPlatformPackage(version) {
  const platform = process.platform;
  const arch = process.arch;
  const libc = detectLibc();
  const name = packageName(platform, arch, libc);
  const libcField = platform === "linux" ? [libc === "musl" ? "musl" : "glibc"] : undefined;

  return {
    arch,
    libc,
    libcField,
    name,
    platform,
    version,
  };
}

function preparePlatformPackage(version) {
  const info = currentPlatformPackage(version);
  run("cargo", ["build", "--release", "-p", "peerline-cli"]);

  const packageDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerline-platform-package-"));
  const binDir = path.join(packageDir, "bin");
  const source = path.join(repoRoot, "target", "release", binaryName());
  const destination = path.join(binDir, binaryName());

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
    repository: "https://github.com/peerline/peerline",
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
  const args = ["publish", "--tag", options.tag, "--access", options.access];
  if (options.otp) {
    args.push(`--otp=${options.otp}`);
  }
  run("npm", args);
  const publishedVersion = run("npm", ["view", `peerline@${version}`, "version"], { capture: true }).trim();
  if (publishedVersion !== version) {
    throw new Error(`published version verification failed: expected ${version}, got ${publishedVersion}`);
  }
}

function publishPlatformPackage(platformPackage, options) {
  const packArgs = ["pack", "--dry-run", "--json", platformPackage.packageDir];
  run("npm", packArgs);

  if (!options.publish) {
    console.log(`Skipping npm publish for ${platformPackage.name}@${platformPackage.version}.`);
    return;
  }

  const publishArgs = ["publish", platformPackage.packageDir, "--tag", options.tag, "--access", options.access];
  if (options.otp) {
    publishArgs.push(`--otp=${options.otp}`);
  }
  run("npm", publishArgs);

  const publishedVersion = run("npm", ["view", `${platformPackage.name}@${platformPackage.version}`, "version"], {
    capture: true,
  }).trim();
  if (publishedVersion !== platformPackage.version) {
    throw new Error(
      `platform package verification failed: expected ${platformPackage.version}, got ${publishedVersion}`
    );
  }
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
  const targetVersion = options.current ? versions.version : options.version || nextAlphaVersion(versions.version);
  assertAlphaVersion(targetVersion);

  if (targetVersion !== versions.version) {
    console.log(`Bumping ${versions.version} -> ${targetVersion}`);
    setProjectVersion(targetVersion, versions);
  } else {
    console.log(`Using current version ${targetVersion}`);
  }

  verifyCargoLockVersion(targetVersion);

  if (!options.skipTests) {
    run("npm", ["test"]);
  }

  let platformPackage = null;
  try {
    if (targetIncludes(options, "platform")) {
      platformPackage = preparePlatformPackage(targetVersion);
      publishPlatformPackage(platformPackage, { ...options, publish: false });
    }

    if (targetIncludes(options, "main")) {
      run("npm", ["pack", "--dry-run", "--json"]);
      run("node", ["-e", "const shim = require('./npm/peerline.js'); console.log(shim.packageName());"]);
    }

    if (options.commit) {
      commitRelease(targetVersion);
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
  cargoWorkspaceVersion,
  nextAlphaVersion,
  parseArgs,
  platformPackageNames,
  release,
  setCargoWorkspaceVersion,
  setPlatformDependencyVersions,
};
