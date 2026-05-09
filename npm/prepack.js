#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const bundledBinaryDir = path.join(__dirname, "bin");

if (fs.existsSync(bundledBinaryDir)) {
  fs.rmSync(bundledBinaryDir, { recursive: true, force: true });
}

console.log("peerline main package uses platform optional dependencies; no host binary is bundled.");
