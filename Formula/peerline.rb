# frozen_string_literal: true

class Peerline < Formula
  desc "Terminal-first peer-to-peer file transfer CLI"
  homepage "https://github.com/backrunner/peerline"
  version "0.1.1-beta.3"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.3/peerline-darwin-arm64", using: :nounzip
      sha256 "ab92eb9a5c372b9be6efb3a4bcf4c14ffbd704159b63e07f2bf77e5469a13bc7"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.3/peerline-darwin-x64", using: :nounzip
      sha256 "6351796bd69b1aa4f28f2d2bb4c48eff0d728ac22d52124fa316279409d3504d"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.3/peerline-linux-arm64-gnu", using: :nounzip
      sha256 "d9b55952bf481e6478bfab655371d5dc224c194cec5cdffe9ac8aabfe59ffb4b"
    end

    on_intel do
      url "https://github.com/backrunner/peerline/releases/download/v0.1.1-beta.3/peerline-linux-x64-gnu", using: :nounzip
      sha256 "1029945ba8a613c50e4ab3fc140bfad8da8d20061122dd0342618cd0c5b1e019"
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
