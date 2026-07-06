#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const repoRoot = path.resolve(__dirname, "..");
const packageJsonPath = path.join(repoRoot, "package.json");

const formulaAssets = [
  "peerline-darwin-arm64",
  "peerline-darwin-x64",
  "peerline-linux-arm64-gnu",
  "peerline-linux-x64-gnu",
];

function usage() {
  return [
    "Usage: node npm/homebrew-formula.js [options]",
    "",
    "Options:",
    "  --dist <dir>             Directory containing release assets and .sha256 files.",
    "  --output <path>          Write the formula to this path instead of stdout.",
    "  --version <version>      Version to publish. Defaults to package.json version.",
    "  --release-base-url <url> Base URL for release assets.",
    "  --help                   Show this help.",
  ].join("\n");
}

function parseArgs(argv) {
  const options = {
    distDir: path.join(repoRoot, "dist"),
    help: false,
    output: "",
    releaseBaseUrl: process.env.PEERLINE_RELEASE_BASE_URL || "",
    version: "",
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

    if (arg === "--help" || arg === "-h") {
      options.help = true;
    } else if (arg === "--dist" || arg.startsWith("--dist=")) {
      options.distDir = path.resolve(readValue("--dist"));
    } else if (arg === "--output" || arg.startsWith("--output=")) {
      options.output = path.resolve(readValue("--output"));
    } else if (arg === "--version" || arg.startsWith("--version=")) {
      options.version = readValue("--version");
    } else if (arg === "--release-base-url" || arg.startsWith("--release-base-url=")) {
      options.releaseBaseUrl = readValue("--release-base-url");
    } else {
      throw new Error(`unknown option: ${arg}`);
    }
  }

  return options;
}

function readPackageJson(fsImpl = fs) {
  return JSON.parse(fsImpl.readFileSync(packageJsonPath, "utf8"));
}

function packageVersion(fsImpl = fs) {
  return readPackageJson(fsImpl).version;
}

function repositorySlug(packageJson = readPackageJson()) {
  const repository = typeof packageJson.repository === "string"
    ? packageJson.repository
    : packageJson.repository?.url;
  if (!repository) {
    throw new Error("package.json repository is required to build the Homebrew formula URL");
  }

  const normalized = repository
    .replace(/^git\+/, "")
    .replace(/\.git$/, "")
    .replace(/^https:\/\/github\.com\//, "")
    .replace(/^git@github\.com:/, "");
  const match = normalized.match(/^([^/]+\/[^/#]+)$/);
  if (!match) {
    throw new Error(`unsupported GitHub repository URL: ${repository}`);
  }
  return match[1];
}

function defaultReleaseBaseUrl(version, packageJson = readPackageJson()) {
  return `https://github.com/${repositorySlug(packageJson)}/releases/download/v${version}`;
}

function normalizeReleaseBaseUrl(url) {
  return url.replace(/\/+$/, "");
}

function parseChecksumLine(line, checksumPath) {
  const match = line.trim().match(/^([a-fA-F0-9]{64})\s+\*?(.+)$/);
  if (!match) {
    throw new Error(`invalid sha256 file: ${checksumPath}`);
  }
  return {
    assetName: path.basename(match[2]),
    sha256: match[1].toLowerCase(),
  };
}

function readChecksums(distDir, assets = formulaAssets, fsImpl = fs) {
  const checksums = {};
  for (const assetName of assets) {
    const checksumPath = path.join(distDir, `${assetName}.sha256`);
    if (!fsImpl.existsSync(checksumPath)) {
      throw new Error(`missing checksum file: ${checksumPath}`);
    }

    const firstLine = fsImpl.readFileSync(checksumPath, "utf8").split(/\r?\n/).find(Boolean) || "";
    const parsed = parseChecksumLine(firstLine, checksumPath);
    if (parsed.assetName !== assetName) {
      throw new Error(`checksum file ${checksumPath} is for ${parsed.assetName}, expected ${assetName}`);
    }
    checksums[assetName] = parsed.sha256;
  }
  return checksums;
}

function checksumFor(checksums, assetName) {
  const checksum = checksums[assetName];
  if (!checksum) {
    throw new Error(`missing sha256 for ${assetName}`);
  }
  return checksum;
}

function generateHomebrewFormula({
  checksums,
  releaseBaseUrl,
  version,
  homepage = "https://github.com/backrunner/peerline",
}) {
  const baseUrl = normalizeReleaseBaseUrl(releaseBaseUrl);
  const assetUrl = (assetName) => `${baseUrl}/${assetName}`;
  const sha = (assetName) => checksumFor(checksums, assetName);

  return `${[
    "# frozen_string_literal: true",
    "",
    "class Peerline < Formula",
    '  desc "Terminal-first peer-to-peer file transfer CLI"',
    `  homepage "${homepage}"`,
    `  version "${version}"`,
    '  license "Apache-2.0"',
    "",
    "  on_macos do",
    "    on_arm do",
    `      url "${assetUrl("peerline-darwin-arm64")}", using: :nounzip`,
    `      sha256 "${sha("peerline-darwin-arm64")}"`,
    "    end",
    "",
    "    on_intel do",
    `      url "${assetUrl("peerline-darwin-x64")}", using: :nounzip`,
    `      sha256 "${sha("peerline-darwin-x64")}"`,
    "    end",
    "  end",
    "",
    "  on_linux do",
    "    on_arm do",
    `      url "${assetUrl("peerline-linux-arm64-gnu")}", using: :nounzip`,
    `      sha256 "${sha("peerline-linux-arm64-gnu")}"`,
    "    end",
    "",
    "    on_intel do",
    `      url "${assetUrl("peerline-linux-x64-gnu")}", using: :nounzip`,
    `      sha256 "${sha("peerline-linux-x64-gnu")}"`,
    "    end",
    "  end",
    "",
    "  def install",
    '    asset = Dir["peerline-*"].find { |path| File.file?(path) }',
    '    bin.install asset => "peerline"',
    "  end",
    "",
    "  test do",
    '    assert_match version.to_s, shell_output("#{bin}/peerline --version")',
    "  end",
    "end",
  ].join("\n")}\n`;
}

function writeFormula(formula, output, fsImpl = fs) {
  if (!output) {
    process.stdout.write(formula);
    return;
  }
  fsImpl.mkdirSync(path.dirname(output), { recursive: true });
  fsImpl.writeFileSync(output, formula);
}

function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(usage());
    return;
  }

  const pkg = readPackageJson();
  const version = options.version || pkg.version;
  const releaseBaseUrl = options.releaseBaseUrl || defaultReleaseBaseUrl(version, pkg);
  const checksums = readChecksums(options.distDir);
  const formula = generateHomebrewFormula({
    checksums,
    homepage: `https://github.com/${repositorySlug(pkg)}`,
    releaseBaseUrl,
    version,
  });
  writeFormula(formula, options.output);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = {
  defaultReleaseBaseUrl,
  formulaAssets,
  generateHomebrewFormula,
  normalizeReleaseBaseUrl,
  parseArgs,
  parseChecksumLine,
  readChecksums,
  repositorySlug,
};
