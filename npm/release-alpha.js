#!/usr/bin/env node
"use strict";

const { release } = require("./release.js");

if (require.main === module) {
  try {
    release(["--channel=alpha", ...process.argv.slice(2)]);
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = require("./release.js");
