# frozen_string_literal: true

class Peerline < Formula
  desc "Terminal-first peer-to-peer file transfer CLI"
  homepage "https://github.com/backrunner/peerline"
  version "0.1.1-beta.1"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.1/peerline-darwin-arm64", using: :nounzip
      sha256 "2f36de56534a4d4df394cd4c8c258d0897ea856589d64ecb09d85165d29f5177"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.1/peerline-darwin-x64", using: :nounzip
      sha256 "7e980acf58a6f1927091b0b7c11bbd45fc9fb7e86846fd30bf46a790a0c2b0d3"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.1/peerline-linux-arm64-gnu", using: :nounzip
      sha256 "6d9e9e7ef19ba529b9acddd9d77d72683f725a3ba2f5d3c2b06f79e005e17e64"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.1/peerline-linux-x64-gnu", using: :nounzip
      sha256 "0024cb16f90798a7a5612cda6cd6e430c2ce1b4cfbfc120a77395daf984f8fef"
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
