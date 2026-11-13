fn main() {
    println!("cargo:rerun-if-changed=Info.plist");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        println!(
            "cargo:rustc-link-arg-bin=peerline=-Wl,-sectcreate,__TEXT,__info_plist,{manifest}/Info.plist"
        );
    }
}
