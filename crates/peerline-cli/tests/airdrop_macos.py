import concurrent.futures, fcntl, http.client, os, pathlib, plistlib, pty, re, select, signal, socket, ssl, subprocess, tempfile, termios, time

# Opt-in native test: requires macOS, authorized Bluetooth, Wi-Fi and a built CLI.
ROOT = pathlib.Path(__file__).resolve().parents[3]
binary = str(ROOT / 'target/debug/peerline')
fixture = (ROOT / 'crates/peerline-net/tests/fixtures/airdrop-odc.cpio.gz').read_bytes()
master, slave = pty.openpty()
def setup():
    os.setsid()
    fcntl.ioctl(slave, termios.TIOCSCTTY, 0)

with tempfile.TemporaryDirectory(prefix='peerline-airdrop-pty-') as root:
    proc = subprocess.Popen([binary, 'recv', '--airdrop', '--idle-timeout-minutes', '1'], stdin=slave, stdout=slave, stderr=slave, cwd=root, preexec_fn=setup)
    os.close(slave)
    output = bytearray()
    def wait_text(text, seconds=12):
        end = time.monotonic() + seconds
        while text.encode() not in output:
            if time.monotonic() > end: raise AssertionError('Timed out: '+text+'; '+output.decode(errors='replace'))
            if select.select([master], [], [], .1)[0]: output.extend(os.read(master, 65536))
    try:
        wait_text('AirDrop receiver:')
        children = subprocess.check_output(['ps','-axo','pid,ppid,command'], text=True).splitlines()
        ad = next(l.split() for l in children if len(l.split()) > 2 and l.split()[1] == str(proc.pid) and '/usr/bin/dns-sd' in l)
        port = int(ad[ad.index('local.')+1])
        text = subprocess.check_output(['/sbin/ifconfig','awdl0'], text=True)
        ip = re.search(r'inet6 (fe80:[^% ]+)',text)[1]
        index = socket.if_nametoindex('awdl0')
        def post(path, data, ctype='application/octet-stream'):
            raw = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            raw.settimeout(12)
            raw.setsockopt(socket.SOL_SOCKET,0x1104,1)
            raw.bind((ip, 0, 0, index))
            raw.connect((ip, port, 0, index))
            # This local protocol probe intentionally uses the receiver's ephemeral certificate.
            tls = ssl._create_unverified_context().wrap_socket(raw, server_hostname='peerline.local')
            with tls:
                tls.sendall(f'POST {path} HTTP/1.1\r\nHost: peerline.local\r\nContent-Type: {ctype}\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n'.encode()+data)
                response = http.client.HTTPResponse(tls); response.begin(); content=response.read()
                return response.status, content
        meta = plistlib.dumps({'SenderComputerName':'Local test sender','Files':[{'FileName':'photo.jpg'}]},fmt=plistlib.FMT_BINARY)
        assert post('/Discover',meta)[0] == 200
        os.write(master,b'y\n') # Stale input must not approve the later offer.
        with concurrent.futures.ThreadPoolExecutor() as pool:
            answer = pool.submit(post,'/Ask',meta)
            wait_text('Accept these files?')
            time.sleep(.4)
            assert not answer.done(), 'stale key approved the offer'
            os.write(master,b'y')
            assert answer.result(timeout=12)[0] == 200
        assert post('/Upload',fixture,'application/x-cpio')[0] == 200
        wait_text('AirDrop files saved in')
        files=list(pathlib.Path(root).glob('AirDrop-*/photo.jpg'))
        assert len(files)==1 and files[0].read_bytes()==b'AirDrop fixture\n'
        os.killpg(proc.pid,signal.SIGINT)
        assert proc.wait(timeout=10)==0
        print('PASS: AWDL HTTPS discovery, stale-input rejection, real terminal approval, file equality and Ctrl-C shutdown')
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid,signal.SIGTERM); proc.wait(timeout=10)
        os.close(master)
