#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { platformPackageInfo } = require("./release.js");
const { packageName, releaseAssetName } = require("./peerline.js");

const repoRoot = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const options = {
    platformPackage: process.env.PEERLINE_PLATFORM_PACKAGE || "",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const readValue = (name) => {
      const inlinePrefix = `${name}=`;
      if (arg.startsWith(inlinePrefix)) return arg.slice(inlinePrefix.length);
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) throw new Error(`${name} requires a value`);
      index += 1;
      return value;
    };

    if (arg === "--platform-package" || arg.startsWith("--platform-package=")) {
      options.platformPackage = readValue("--platform-package");
    } else {
      throw new Error(`unknown option: ${arg}`);
    }
  }

  return options;
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
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} exited with ${result.status}`);
  return result.stdout || "";
}

function sha256(filePath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("hex");
}

function buildPlatformAsset(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  const version = require("../package.json").version;
  const info = platformPackageInfo(options.platformPackage || packageName(), version);
  const buildArgs = ["build", "--release", "-p", "peerline-cli"];
  if (info.cargoTarget) buildArgs.push("--target", info.cargoTarget);
  run("cargo", buildArgs);

  const releaseDir = info.cargoTarget
    ? path.join(repoRoot, "target", info.cargoTarget, "release")
    : path.join(repoRoot, "target", "release");
  const source = path.join(releaseDir, info.binaryName);
  if (!fs.existsSync(source)) {
    throw new Error(`missing release binary: ${source}`);
  }

  const distDir = path.join(repoRoot, "dist");
  fs.rmSync(distDir, { recursive: true, force: true });
  fs.mkdirSync(distDir, { recursive: true });
  const assetName = releaseAssetName(info.name, info.binaryName);
  const assetPath = path.join(distDir, assetName);
  fs.copyFileSync(source, assetPath);
  if (info.platform !== "win32") fs.chmodSync(assetPath, 0o755);

  const checksumPath = `${assetPath}.sha256`;
  fs.writeFileSync(checksumPath, `${sha256(assetPath)}  ${assetName}\n`);
  console.log(`Built ${assetPath}`);
  console.log(`Wrote ${checksumPath}`);
}

if (require.main === module) {
  try {
    buildPlatformAsset();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = {
  buildPlatformAsset,
  parseArgs,
};
