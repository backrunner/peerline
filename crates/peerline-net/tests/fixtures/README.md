# AirDrop CPIO fixtures

`airdrop-odc.cpio.gz` and `airdrop-newc.cpio.gz` were produced with macOS
`/usr/bin/tar` (libarchive), not the parser under test. Both contain a single
`photo.jpg` with the bytes `AirDrop fixture\n`, including the trailing newline.

Generation from a temporary directory containing that file:

```sh
/usr/bin/tar -czf airdrop-odc.cpio.gz --format=odc --options gzip:timestamp=0 -b 1 photo.jpg
/usr/bin/tar -czf airdrop-newc.cpio.gz --format=newc --options gzip:timestamp=0 -b 1 photo.jpg
```

The gzip output is padded to a 512-byte block. Tests verify both the archive
format and libarchive's outer padding, and reject tampered or truncated gzip.

`airdrop-directory.cpio.gz` uses the same odc command with `./folder`. It contains
`folder/empty` (an empty directory) and `folder/notes.txt` (`folder fixture\n`).
