# frozen_string_literal: true

class Peerline < Formula
  desc "Terminal-first peer-to-peer file transfer CLI"
  homepage "https://github.com/backrunner/peerline"
  version "0.1.1-beta.2"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.2/peerline-darwin-arm64", using: :nounzip
      sha256 "cb221db8dd6d3748e4a6772d35779fd7fbe9c7c7224e4be3d26140fb0ebc5798"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.2/peerline-darwin-x64", using: :nounzip
      sha256 "973c035889dfa667f14e71d86b845422382e4737e335d5dfa819c0f8e4addfb2"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.2/peerline-linux-arm64-gnu", using: :nounzip
      sha256 "a00c7031b56a70bad205f3f082ec26645b392821bfcab19a0a23715008962d94"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.2/peerline-linux-x64-gnu", using: :nounzip
      sha256 "dafc52b26ca5dd51da3e58d45481ad6e70650efd4b922a6dfd0f92e54adf2a6d"
    end
  end

  def install
    asset = Dir["peerline-*"].find { |path| File.file?(path) }
    bin.install asset => "peerline"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/peerline --version")
  end
end
