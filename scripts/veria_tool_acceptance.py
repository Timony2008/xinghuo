#!/usr/bin/env python3
"""Offline functional acceptance, executed by a real member's Python inside Linux.

No models, benchmark APIs, remote targets, downloads or flags are used. Run the
outer container with --network none. The networking fixture binds loopback only.
Every functional case runs in its own temporary directory/process; failures are
recorded independently and cause a nonzero exit. Nested container restrictions
are recorded separately from installation success, never as a passed runtime.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time


MANIFEST = Path("/opt/xinghuo/tool-manifest.json")
CASE_TIMEOUTS = {"crypto_math": 120, "sage_flatter": 180, "rsa": 120,
                 "cado": 600,
                 "binary": 90, "tracing": 90, "angr": 180, "ghidra": 300,
                 "steganography_ocr": 120, "media": 90,
                 "forensics": 180, "machine_learning": 180,
                 "networking": 60, "utilities": 180}


def execute(argv, *, timeout=60, input=None, check=True):
    """Bound the full process group, including Java/solver descendants."""
    started = time.monotonic()
    child = subprocess.Popen([str(part) for part in argv], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, start_new_session=True)
    try:
        stdout, stderr = child.communicate(input=input, timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        stdout, stderr = child.communicate()
        result = {"returncode": 124, "timeout": True, "stdout": stdout[-12000:],
                "stderr": stderr[-12000:], "seconds": round(time.monotonic() - started, 3)}
        if check:
            raise RuntimeError(json.dumps({"argv": list(map(str, argv)), **result}))
        return result
    result = {"returncode": child.returncode, "stdout": stdout[-12000:],
              "stderr": stderr[-12000:], "seconds": round(time.monotonic() - started, 3)}
    if check and child.returncode:
        raise RuntimeError(json.dumps({"argv": list(map(str, argv)), **result}))
    return result


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def network_state():
    interfaces, errors = {}, {}
    for path in Path("/sys/class/net").glob("*"):
        if path.name == "bonding_masters":
            continue  # Kernel control file, not a network interface.
        try:
            flags = int((path / "flags").read_text().strip(), 16)
            interfaces[path.name] = {"flags": flags, "up": bool(flags & 1), "loopback": bool(flags & 8)}
        except OSError as error:
            errors[path.name] = str(error)
    return {"interfaces": interfaces, "errors": errors,
            "active_non_loopback": [name for name, state in interfaces.items() if state["up"] and not state["loopback"]],
            "ipv4_routes": Path("/proc/net/route").read_text() if Path("/proc/net/route").exists() else None}


def compile_fixture():
    Path("fixture.c").write_text("#include <stdio.h>\n"
        "__attribute__((noinline)) int marker(int x) { return x * 7 + 3; }\n"
        'int main(void) { puts("VERIA_OFFLINE_FIXTURE"); return marker(2) == 17 ? 0 : 1; }\n')
    execute(["gcc", "-O0", "-g", "-fno-pie", "-no-pie", "fixture.c", "-o", "fixture"])
    return str(Path("fixture").resolve())


def crypto_math():
    from Crypto.Cipher import AES
    import gmpy2
    import jwt
    import numpy as np
    from scipy.linalg import solve
    import sympy
    import z3
    from fpylll import IntegerMatrix, LLL
    from pwn import p64, u64
    from capstone import Cs, CS_ARCH_X86, CS_MODE_64
    from unicorn import Uc, UC_ARCH_X86, UC_MODE_64
    from unicorn.x86_const import UC_X86_REG_RAX
    key, nonce, plain = b"0" * 16, b"1" * 12, b"offline synthetic fixture"
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(plain)
    check(AES.new(key, AES.MODE_GCM, nonce=nonce).decrypt_and_verify(ciphertext, tag) == plain, "AES roundtrip")
    check(int(gmpy2.invert(17, 3120)) == 2753, "modular inverse")
    check(sympy.factorint(10403) == {101: 1, 103: 1}, "integer factoring")
    check(np.allclose(solve([[2., 0.], [0., 3.]], [4., 9.]), [2., 3.]), "scipy solve")
    solver, x = z3.Solver(), z3.Int("x")
    solver.add(7 * x + 3 == 17)
    check(solver.check() == z3.sat and solver.model()[x].as_long() == 2, "z3 solve")
    matrix = IntegerMatrix.from_matrix([[10000, 1], [1, 0]])
    LLL.reduction(matrix)
    check(abs(matrix[0, 0] * matrix[1, 1] - matrix[0, 1] * matrix[1, 0]) == 1, "fpylll determinant")
    check(max(abs(matrix[i, j]) for i in range(2) for j in range(2)) == 1, "fpylll reduction")
    token = jwt.encode({"fixture": 17}, "offline-test-key-at-least-32-bytes", algorithm="HS256")
    check(jwt.decode(token, "offline-test-key-at-least-32-bytes", algorithms=["HS256"])["fixture"] == 17, "JWT")
    check(u64(p64(0x1020304050607080)) == 0x1020304050607080, "pwntools packing")
    code = bytes.fromhex("48c7c02a000000")
    check(next(Cs(CS_ARCH_X86, CS_MODE_64).disasm(code, 0x1000)).mnemonic == "mov", "capstone disassembly")
    emulator = Uc(UC_ARCH_X86, UC_MODE_64)
    emulator.mem_map(0x1000, 0x1000)
    emulator.mem_write(0x1000, code)
    emulator.emu_start(0x1000, 0x1000 + len(code))
    check(emulator.reg_read(UC_X86_REG_RAX) == 42, "unicorn execution")
    return {"verified": ["AES", "RSA arithmetic", "SymPy", "SciPy", "NumPy", "Z3", "fpylll LLL", "JWT", "pwntools", "Capstone", "Unicorn"]}


def sage_flatter():
    sage = execute(["sage", "-c", "from sage.all import *; n=10403; assert prod(ecm.factor(n))==n; M=matrix(ZZ,[[10000,1],[1,0]]); R=M.LLL(); assert abs(R.det())==1 and max(map(abs,R.list()))==1; print('SAGE_ECM_LLL_OK')"], timeout=160)
    check("SAGE_ECM_LLL_OK" in sage["stdout"], "Sage ECM/LLL result")
    flatter = execute(["flatter"], input="[[10000 1]\n[1 0]]\n")
    values = list(map(int, re.findall(r"-?\d+", flatter["stdout"])))
    check(len(values) == 4 and abs(values[0] * values[3] - values[1] * values[2]) == 1 and max(map(abs, values)) == 1, "flatter reduction")
    return {"verified": ["Sage ECM", "Sage LLL", "flatter LLL"]}


def cado():
    from sympy import nextprime
    p, q = int(nextprime(10**14)), int(nextprime(10**14 + 1000))
    parameters = list(Path("/opt/cado-nfs").rglob("params.c30"))
    check(len(parameters) == 1, "CADO installed quick-test parameter set")
    # Upstream params.c30 is expressly intended for a complete short factoring
    # test. Its local server/client exchanges remain entirely on loopback.
    result = execute(["cado-nfs", parameters[0], f"N={p*q}", "tasks.threads=2",
                      "tasks.workdir=" + str(Path("cado-work").resolve()),
                      "server.address=127.0.0.1", "server.ssl=no",
                      "slaves.hostnames=localhost", "slaves.nrclients=1"], timeout=570)
    numbers = {int(value) for value in re.findall(r"\b\d{14,16}\b", result["stdout"])}
    check(p in numbers and q in numbers, "CADO must output both computed prime factors")
    return {"verified": ["CADO-NFS complete factoring"], "modulus": p*q,
            "factors": [p, q], "stdout": result["stdout"][-4000:]}


def rsa():
    from Crypto.PublicKey import RSA
    command = shutil.which("RsaCtfTool") or shutil.which("rsactftool")
    check(command, "RsaCtfTool executable")
    result = execute([command, "-n", "1000036000099", "-e", "65537", "--attack", "fermat", "--private"], timeout=100)
    combined = result["stdout"] + result["stderr"]
    pem = re.search(r"-----BEGIN (?:RSA )?PRIVATE KEY-----.*?-----END (?:RSA )?PRIVATE KEY-----", combined, re.S)
    check(pem, "Fermat attack must produce a private key")
    key = RSA.import_key(pem.group(0))
    check(key.n == 1000036000099 and key.has_private() and key.p * key.q == key.n, "RSA factors")
    return {"verified": ["RsaCtfTool Fermat attack"], "modulus": key.n}


def binary():
    binary_path = compile_fixture()
    check("VERIA_OFFLINE_FIXTURE" in execute([binary_path])["stdout"], "compiled executable")
    for argv, expected in [(["file", binary_path], "ELF"), (["readelf", "-h", binary_path], "ELF"),
                           (["nm", binary_path], "marker"), (["strings", binary_path], "VERIA_OFFLINE_FIXTURE"),
                           (["objdump", "-d", binary_path], "marker"),
                           (["r2", "-q", "-c", "aaa;afl;q", binary_path], "marker"),
                           (["gdb", "-batch", "-ex", "file " + binary_path, "-ex", "disassemble marker"], "marker")]:
        check(expected in execute(argv)["stdout"], "binary check: " + argv[0])
    gadgets = execute(["ROPgadget", "--binary", binary_path])["stdout"]
    count = re.search(r"Unique gadgets found:\s*(\d+)", gadgets)
    check(count and int(count.group(1)) > 0 and re.search(r"0x[0-9a-fA-F]+\s*:\s*[^\n]*ret", gadgets), "ROPgadget must discover actual return gadgets")
    return {"verified": ["native ELF compile/run", "binutils", "radare2 analysis", "gdb disassembly", "ROPgadget"]}


def traced_exec_architecture(binary_path):
    """Observe the stopped child, not the emulator's virtualized /proc/self."""
    source = r'''
#include <stdio.h>
#include <unistd.h>
#include <sys/ptrace.h>
#include <sys/wait.h>
#include <signal.h>
#include <string.h>
int main(int argc,char **argv) {
 pid_t pid=fork();
 if(pid==0){if(ptrace(PTRACE_TRACEME,0,0,0)==-1)_exit(3);execl(argv[1],argv[1],NULL);_exit(4);}
 int status=0;if(waitpid(pid,&status,0)<0)return 5;
 if(!WIFSTOPPED(status))return 6;
 char proc[64],link[4096];snprintf(proc,sizeof(proc),"/proc/%d/exe",pid);
 int n=readlink(proc,link,sizeof(link)-1);if(n>=0)link[n]=0;else strcpy(link,"UNREADABLE");
 unsigned char elf[64]={0};FILE *f=fopen(proc,"rb");if(f){fread(elf,1,sizeof(elf),f);fclose(f);}
 printf("ELF_MACHINE=%u EXE=%s\n",elf[18]+256*elf[19],link);fflush(stdout);
 for(int n=0;n<100 && WIFSTOPPED(status);n++){if(ptrace(PTRACE_CONT,pid,0,0)==-1)break;if(waitpid(pid,&status,0)<0)break;}
 if(WIFSTOPPED(status)){kill(pid,SIGKILL);waitpid(pid,&status,0);}
 return 0;
}
'''
    Path("exec-architecture.c").write_text(source)
    execute(["gcc", "-O0", "exec-architecture.c", "-o", "exec-architecture"])
    observed = execute([str(Path("exec-architecture").resolve()), binary_path], timeout=20, check=False)
    found = re.search(r"ELF_MACHINE=(\d+) EXE=([^\n]+)", observed["stdout"])
    with open(binary_path, "rb") as fixture:
        header = fixture.read(20)
    target = int.from_bytes(header[18:20], "little" if header[5] == 1 else "big")
    actual = int(found.group(1)) if found else None
    return {"target_elf_machine": target, "stopped_executable_elf_machine": actual,
            "stopped_executable": found.group(2) if found else None,
            "architecture_translation_observed": actual is not None and actual != target,
            **observed}


def tracing():
    binary_path = compile_fixture()
    subchecks = {}
    architecture_probe = None
    for command, expected in (("strace", "execve("), ("ltrace", "puts(")):
        output = Path(command + ".txt")
        result = execute([command, "-o", output, binary_path], timeout=30, check=False)
        traced = output.read_text(errors="replace") if output.exists() else ""
        combined = (result["stdout"] + result["stderr"] + traced).lower()
        if result["returncode"] == 0 and expected in traced:
            status = "passed"
        elif re.search(r"ptrace[^\n]*(?:function not implemented|operation not permitted|permission denied|is not implemented)", combined):
            status = "environment_restricted"
        else:
            if architecture_probe is None:
                architecture_probe = traced_exec_architecture(binary_path)
            observed_translation_failure = (
                command == "strace" and result["returncode"] == 0
                and "VERIA_OFFLINE_FIXTURE" in result["stdout"] and "+++ exited with 0 +++" in traced
            ) or (command == "ltrace" and "elf from incompatible architecture" in combined)
            status = "environment_restricted" if architecture_probe["architecture_translation_observed"] and observed_translation_failure else "failed"
        subchecks[command] = {"status": status, "trace": traced[-6000:], **result}
        if status == "environment_restricted" and architecture_probe and architecture_probe["architecture_translation_observed"]:
            subchecks[command]["reason"] = "The stopped child executes through a different-architecture translator; native trace output is unavailable in this local emulation."
    restricted = any(item["status"] == "environment_restricted" for item in subchecks.values())
    failed = any(item["status"] == "failed" for item in subchecks.values())
    return {"status": "failed" if failed else "environment_restricted" if restricted else "passed",
            "subchecks": subchecks, "exec_architecture_probe": architecture_probe}


def angr_case():
    import angr
    import claripy
    project = angr.Project(compile_fixture(), auto_load_libs=False)
    symbol = project.loader.find_symbol("marker")
    check(symbol, "marker symbol")
    x = claripy.BVS("synthetic_input", 32)
    state = project.factory.call_state(symbol.rebased_addr, x, prototype="int marker(int x)",
        add_options={angr.options.ZERO_FILL_UNCONSTRAINED_MEMORY, angr.options.ZERO_FILL_UNCONSTRAINED_REGISTERS})
    simulation = project.factory.simgr(state)
    simulation.run(n=50)
    check(simulation.deadended, "angr must reach function return")
    end = simulation.deadended[0]
    result = end.regs.rax if project.arch.name == "AMD64" else end.regs.x0
    end.solver.add(result[31:0] == 17)
    check(end.solver.satisfiable() and end.solver.eval(x) == 2, "angr solve input")
    return {"verified": ["angr symbolic execution"], "architecture": project.arch.name, "solution": 2}


def ghidra():
    import pyghidra
    binary_path = compile_fixture()
    with pyghidra.open_program(binary_path, project_location=str(Path("ghidra-project").resolve())) as api:
        from ghidra.app.decompiler import DecompInterface
        from ghidra.util.task import ConsoleTaskMonitor
        program = api.currentProgram
        marker = next((function for function in program.getFunctionManager().getFunctions(True)
                       if function.getName() == "marker"), None)
        check(marker is not None, "Ghidra marker function discovery")
        decompiler = DecompInterface()
        try:
            check(decompiler.openProgram(program), "Ghidra decompiler program open")
            result = decompiler.decompileFunction(marker, 90, ConsoleTaskMonitor())
            check(result.decompileCompleted(), "Ghidra decompilation: " + str(result.getErrorMessage()))
            source = str(result.getDecompiledFunction().getC())
            check("marker" in source and "7" in source and "3" in source, "decompiled arithmetic")
        finally:
            decompiler.dispose()
    return {"verified": ["PyGhidra JVM startup", "ELF import/analysis", "native decompiler"], "decompiled_source": source}


def steganography_ocr():
    from PIL import Image, ImageDraw, ImageFont
    import pytesseract
    import random
    payload = b"VERIA_OFFLINE_STEG\n"
    Path("payload.txt").write_bytes(payload)
    Image.frombytes("RGB", (512, 512), random.Random(1701).randbytes(512 * 512 * 3)).save("cover.jpg", quality=95)
    execute(["steghide", "embed", "-cf", "cover.jpg", "-ef", "payload.txt", "-sf", "hidden.jpg", "-p", "offline-pass", "-f"])
    execute(["steghide", "extract", "-sf", "hidden.jpg", "-xf", "restored.txt", "-p", "offline-pass", "-f"])
    check(Path("restored.txt").read_bytes() == payload, "steghide roundtrip")
    Path("wordlist.txt").write_text("wrong\noffline-pass\n")
    execute(["stegseek", "hidden.jpg", "wordlist.txt", "cracked.txt", "-f"])
    check(Path("cracked.txt").read_bytes() == payload, "stegseek password recovery")
    raw = bytearray([128] * (128 * 128 * 3))
    for index, bit in enumerate(f"{int.from_bytes(payload, 'big'):0{len(payload)*8}b}"):
        raw[index] |= int(bit)
    Image.frombytes("RGB", (128, 128), bytes(raw)).save("lsb.png")
    decoded = execute(["zsteg", "-E", "b1,rgb,lsb,xy", "lsb.png"])["stdout"]
    check(decoded.startswith(payload.decode()), "zsteg LSB extraction")
    execute(["pngcheck", "-v", "lsb.png"])
    execute(["exiftool", "-overwrite_original", "-Comment=offline-fixture", "cover.jpg"])
    check("offline-fixture" in execute(["exiftool", "-Comment", "cover.jpg"])["stdout"], "metadata roundtrip")
    fonts = list(Path("/usr/share/fonts").rglob("DejaVuSans.ttf"))
    check(fonts, "OCR test font")
    picture = Image.new("RGB", (1000, 160), "white")
    ImageDraw.Draw(picture).text((20, 20), "TEST 123", font=ImageFont.truetype(str(fonts[0]), 72), fill="black")
    picture.save("ocr.png")
    check("TEST 123" in pytesseract.image_to_string(picture, config="--psm 6"), "OCR actual recognition")
    execute(["convert", "ocr.png", "-resize", "50%", "small.png"])
    execute(["montage", "ocr.png", "small.png", "-geometry", "+0+0", "montage.png"])
    check("PNG" in execute(["identify", "montage.png"])["stdout"], "ImageMagick")
    return {"verified": ["Pillow", "steghide", "stegseek", "zsteg", "pngcheck", "exiftool", "Tesseract/pytesseract", "ImageMagick"]}


def media():
    import math
    import struct
    import wave
    with wave.open("tone.wav", "wb") as audio:
        audio.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
        audio.writeframes(b"".join(struct.pack("<h", int(10000 * math.sin(2 * math.pi * 440 * i / 8000))) for i in range(4000)))
    execute(["ffmpeg", "-nostdin", "-v", "error", "-i", "tone.wav", "-y", "tone.flac"])
    probe = json.loads(execute(["ffprobe", "-v", "error", "-show_streams", "-of", "json", "tone.flac"])["stdout"])
    check(probe["streams"][0]["codec_name"] == "flac", "FFprobe must identify encoded stream")
    execute(["sox", "tone.wav", "-n", "spectrogram", "-o", "spectrum.png"])
    check(Path("tone.flac").stat().st_size > 100 and Path("spectrum.png").stat().st_size > 100, "media artifacts")
    return {"verified": ["FFmpeg encoding", "FFprobe stream parsing", "SoX spectrogram"]}


def forensics():
    from PIL import Image
    Path("proof.txt").write_text("OFFLINE_FILE_RECOVERY\n")
    with Path("filesystem.ext2").open("wb") as image:
        image.truncate(16 * 1024 * 1024)
    execute(["mkfs.ext2", "-q", "-F", "filesystem.ext2"])
    execute(["debugfs", "-w", "-R", "write proof.txt /proof.txt", "filesystem.ext2"])
    listed = execute(["fls", "filesystem.ext2"])["stdout"]
    inode = re.search(r"(?:\S+/\S+)\s+(\d+):\s+proof\.txt", listed)
    check(inode, "Sleuth Kit file discovery")
    check(execute(["icat", "filesystem.ext2", inode.group(1)])["stdout"] == Path("proof.txt").read_text(), "Sleuth Kit extraction")
    execute(["fsstat", "filesystem.ext2"])
    execute(["tsk_recover", "-e", "filesystem.ext2", "recovered"])
    check((Path("recovered") / "proof.txt").read_text() == Path("proof.txt").read_text(), "TSK recovery")
    import struct
    mbr = bytearray(512)
    mbr[446:462] = struct.pack("<B3sB3sII", 0, b"\0" * 3, 0x83, b"\0" * 3, 2048, 32768)
    mbr[510:512] = b"\x55\xaa"
    with Path("partitioned.img").open("wb") as output:
        output.write(mbr)
        output.seek(2048 * 512)
        output.write(Path("filesystem.ext2").read_bytes())
    check("2048" in execute(["mmls", "partitioned.img"])["stdout"], "mmls partition discovery")
    partition = execute(["testdisk", "/list", "partitioned.img"])
    check("Linux" in partition["stdout"], "TestDisk partition discovery")
    execute(["dcfldd", "if=proof.txt", "of=copy.txt", "hash=md5"])
    check(Path("copy.txt").read_bytes() == Path("proof.txt").read_bytes(), "forensic copy")
    Image.new("RGB", (100, 100), "red").save("carve.jpg")
    Path("carve.bin").write_bytes(b"\0" * 4096 + Path("carve.jpg").read_bytes() + b"\0" * 4096)
    execute(["foremost", "-t", "jpg", "-i", "carve.bin", "-o", "carved"])
    check(list(Path("carved/jpg").glob("*.jpg")), "foremost carving")
    check("JPEG" in execute(["binwalk", "carve.bin"])["stdout"], "binwalk embedded signature")
    with Path("filesystem.xfs").open("wb") as image:
        image.truncate(512 * 1024 * 1024)
    execute(["mkfs.xfs", "-f", "filesystem.xfs"])
    check("magicnum" in execute(["xfs_db", "-r", "-c", "sb 0", "-c", "p magicnum", "filesystem.xfs"])["stdout"], "XFS metadata")
    execute(["xfs_repair", "-n", "filesystem.xfs"], timeout=90)
    banner = b"Linux version 6.8.0-veria (fixture@offline) (gcc version 11.4.0) #1 SMP Thu Oct 1 00:00:00 UTC 2026\n"
    Path("memory.raw").write_bytes(b"\0" * 4096 + banner + b"\0" * 65536)
    memory = execute(["vol", "--offline", "-q", "-f", "memory.raw", "banners.Banners"])
    check("6.8.0-veria" in memory["stdout"], "Volatility must scan synthetic memory and recover the banner")
    return {"verified": ["ext2 synthetic filesystem", "TSK listing/extraction/recovery", "mmls partition parsing", "TestDisk partition parsing", "dcfldd", "foremost", "binwalk", "XFS inspection", "Volatility synthetic memory banner scan"], "real_memory_dump": "not_used"}


def machine_learning():
    import numpy as np
    import torch
    import keras
    check(keras.backend.backend() == "torch", "Keras torch backend")
    check(torch.version.cuda is None and torch.tensor([1.]).device.type == "cpu", "CPU-only PyTorch build")
    check(float(torch.tensor([[4., 5.]]) @ torch.tensor([[2.], [3.]])) == 23., "Torch CPU inference")
    model = keras.Sequential([keras.Input(shape=(2,)), keras.layers.Dense(1, use_bias=False)])
    model.set_weights([np.array([[2.], [3.]], dtype="float32")])
    result = keras.ops.convert_to_numpy(model(np.array([[4., 5.]], dtype="float32")))
    check(float(result[0, 0]) == 23., "Keras inference")
    return {"verified": ["CPU PyTorch", "Keras inference"], "backend": keras.backend.backend()}


def networking():
    import http.server
    import threading
    import requests
    from flask import Flask
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OFFLINE_LOOPBACK")
        def log_message(self, *_):
            pass
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/"
    try:
        check(requests.get(url, timeout=3).text == "OFFLINE_LOOPBACK", "requests loopback")
        check(execute(["curl", "--noproxy", "*", "-fsS", url])["stdout"] == "OFFLINE_LOOPBACK", "curl loopback")
        check(execute(["wget", "--no-proxy", "-qO-", url])["stdout"] == "OFFLINE_LOOPBACK", "wget loopback")
        execute(["nc", "-z", "-w", "3", "127.0.0.1", str(server.server_port)])
        check("open" in execute(["nmap", "-sT", "-Pn", "-n", "-p", str(server.server_port), "127.0.0.1"])["stdout"], "nmap loopback")
    finally:
        server.shutdown()
        server.server_close()
    app = Flask(__name__)
    app.add_url_rule("/", view_func=lambda: "LOCAL_FIXTURE")
    check(app.test_client().get("/").data == b"LOCAL_FIXTURE", "Flask local request")
    return {"verified": ["requests", "curl", "wget", "nc", "nmap", "Flask"], "target": "127.0.0.1 synthetic server only"}


def utilities():
    Path("plain.txt").write_text("offline archive fixture")
    execute(["zip", "archive.zip", "plain.txt"])
    execute(["unzip", "-d", "unzipped", "archive.zip"])
    check(Path("unzipped/plain.txt").read_text() == Path("plain.txt").read_text(), "archive roundtrip")
    check(execute(["jq", ".value"], input='{"value":17}')["stdout"].strip() == "17", "jq")
    check("6f66" in execute(["xxd", "plain.txt"])["stdout"], "xxd")
    check("offline" in execute(["hexdump", "-C", "plain.txt"])["stdout"], "hexdump")
    digest = execute(["openssl", "dgst", "-sha256", "plain.txt"])["stdout"]
    import hashlib
    check(hashlib.sha256(Path("plain.txt").read_bytes()).hexdigest() in digest, "openssl hash")
    execute(["git", "init", "-q", "repo"])
    execute(["uv", "venv", "--offline", "--python", sys.executable, "uv-environment"])
    check(execute(["uv-environment/bin/python", "-c", "print(6 * 7)"])["stdout"].strip() == "42", "uv private environment execution")
    package = Path("synthetic-package")
    package.mkdir()
    (package / "setup.py").write_text('from setuptools import setup\nsetup(name="veria-offline-fixture", version="1.0", py_modules=["veria_fixture"], entry_points={"console_scripts":["veria-fixture=veria_fixture:main"]})\n')
    (package / "veria_fixture.py").write_text('def main():\n    print("OFFLINE_PACKAGE_OK")\n')
    execute([sys.executable, "-m", "pip", "wheel", "--no-index", "--no-build-isolation", "--no-deps", "-w", "wheelhouse", package.resolve()])
    wheel = next(Path("wheelhouse").glob("*.whl"))
    execute([sys.executable, "-m", "pip", "install", "--no-index", "--no-deps", "--target", "pip-target", wheel])
    check(Path("pip-target/veria_fixture.py").exists(), "pip offline wheel install")
    check("OFFLINE_PACKAGE_OK" in execute(["uvx", "--offline", "--python", sys.executable, "--from", wheel.resolve(), "veria-fixture"])["stdout"], "uvx offline local tool execution")
    execute(["ruby", "-e", 'require "zsteg"; puts "RUBY_GEM_OK"'])
    from tqdm import tqdm
    check(sum(tqdm(range(7), disable=True)) == 21, "tqdm iteration")
    Path("CMakeLists.txt").write_text('cmake_minimum_required(VERSION 3.16)\nproject(fixture CXX)\nadd_executable(cpp-fixture fixture.cpp)\n')
    Path("fixture.cpp").write_text('#include <iostream>\nint main(){std::cout << 42 << std::endl;}\n')
    execute(["cmake", "-S", ".", "-B", "cpp-build"])
    execute(["cmake", "--build", "cpp-build", "--", "-j1"])
    check(execute(["cpp-build/cpp-fixture"])["stdout"].strip() == "42", "C++/CMake/Make build")
    execute(["pkg-config", "--libs", "gmp"])
    Path("OfflineFixture.java").write_text('class OfflineFixture { public static void main(String[] args) {System.out.println("JAVA_OK");}}\n')
    execute(["javac", "OfflineFixture.java"])
    check("JAVA_OK" in execute(["java", "OfflineFixture"])["stdout"], "JDK compile/run")
    return {"verified": ["zip/unzip", "jq", "xxd", "hexdump", "OpenSSL", "git", "uv venv", "uvx local tool", "pip offline build/install", "setuptools/wheel", "Ruby zsteg", "tqdm", "C++/CMake/Make", "pkg-config", "JDK compile/run"]}


CASES = {"crypto_math": crypto_math, "sage_flatter": sage_flatter, "rsa": rsa, "cado": cado,
         "binary": binary, "tracing": tracing, "angr": angr_case, "ghidra": ghidra,
         "steganography_ocr": steganography_ocr, "media": media,
         "forensics": forensics, "machine_learning": machine_learning,
         "networking": networking, "utilities": utilities}

# Published per-tool coverage keeps "installed", "functionally passed", and
# "not run / environment restricted" separate, including helper dependencies.
CASE_COVERAGE = {
    "crypto_math": {"commands": [], "python": ["pwntools", "pycryptodome", "sympy", "gmpy2", "numpy", "scipy", "z3-solver", "fpylll", "cysignals", "PyJWT", "capstone", "unicorn"]},
    "sage_flatter": {"commands": ["sage", "flatter"], "python": []},
    "rsa": {"commands": ["RsaCtfTool"], "python": ["RsaCtfTool"]},
    "cado": {"commands": ["cado-nfs"], "python": []},
    "binary": {"commands": ["file", "readelf", "nm", "strings", "objdump", "r2", "radare2", "gcc", "gdb", "ROPgadget"], "python": ["ROPGadget"]},
    "tracing": {"commands": ["strace", "ltrace"], "python": []},
    "angr": {"commands": [], "python": ["angr"]},
    "ghidra": {"commands": [], "python": ["pyghidra"]},
    "steganography_ocr": {"commands": ["steghide", "stegseek", "zsteg", "pngcheck", "exiftool", "tesseract", "identify", "convert", "montage"], "python": ["Pillow", "pytesseract"]},
    "media": {"commands": ["ffmpeg", "ffprobe", "sox"], "python": []},
    "forensics": {"commands": ["mmls", "fls", "icat", "tsk_recover", "fsstat", "foremost", "testdisk", "xfs_db", "xfs_repair", "mkfs.xfs", "mkfs.ext2", "debugfs", "dcfldd", "binwalk", "vol"], "python": ["volatility3"]},
    "machine_learning": {"commands": [], "python": ["torch", "keras"]},
    "networking": {"commands": ["nc", "curl", "wget", "nmap"], "python": ["requests", "Flask"]},
    "utilities": {"commands": ["bash", "python", "python3", "pip", "pip3", "openssl", "jq", "zip", "unzip", "git", "uv", "uvx", "ruby", "gem", "g++", "make", "cmake", "pkg-config", "xxd", "hexdump", "java", "javac"], "python": ["pip", "setuptools", "wheel", "tqdm"]},
}
NESTED_COMPONENTS = {"podman", "podman-compose", "buildah", "fuse-overlayfs", "slirp4netns"}


def per_tool_results(manifest, report):
    rows = {}
    for kind, inventory_group in (("commands", "commands"), ("python", "python_modules")):
        names = manifest["commands"] if kind == "commands" else manifest["python_modules"]
        for name in names:
            observed = report.get("inventory", {}).get(inventory_group, {}).get(name, {})
            provenance = observed.get("provenance", {})
            case_name = next((case for case, coverage in CASE_COVERAGE.items() if name in coverage[kind]), None)
            result = report.get("functional", {}).get(case_name, {})
            functional = {"status": result.get("status", "not_run"), "case": case_name}
            if functional["status"] == "failed":
                functional.update({"status": "not_run", "reason": "The grouped case failed or stopped early; no individual pass is claimed."})
            if case_name == "tracing":
                trace = (result.get("details") or {}).get("subchecks", {}).get(name)
                if trace:
                    functional["status"] = trace["status"]
            if name in NESTED_COMPONENTS:
                nested = report.get("nested_containers", {})
                component = nested.get("components", {}).get(name, {})
                functional = {"status": component.get("status", nested.get("status", "not_run") if name == "podman" else "not_run"),
                              "case": "nested_containers", "reason": component.get("reason", "Kernel-dependent component; no independent run claimed.")}
            rows[("command:" if kind == "commands" else "python:") + name] = {
                "installed": observed.get("status", "not_checked"), "path": observed.get("path"),
                "version": observed.get("version") or provenance.get("version") or provenance.get("commit"),
                "provenance": provenance, "functional": functional}
    return rows


def inventory(manifest, tools_path=Path("/tools.txt")):
    documentation_error = None
    try:
        tools_digest = hashlib.sha256(tools_path.read_bytes()).hexdigest()
    except OSError as error:
        tools_digest = None
        documentation_error = str(error)
    tools_documentation_ok = tools_digest is not None and tools_digest == manifest.get("tools_txt_sha256")
    commands = {}
    for name in manifest["commands"]:
        path = shutil.which(name)
        provenance = manifest.get("command_provenance", {}).get(name, {})
        digest = None
        if path:
            hasher = hashlib.sha256()
            with Path(path).open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    hasher.update(block)
            digest = hasher.hexdigest()
        # Private member Python/pip wrappers are intentionally distinct from
        # /opt/ctf wrappers. Other installed executables must match build bytes.
        inherited_wrapper = name in {"python", "python3", "pip", "pip3"}
        hash_matches = inherited_wrapper or not provenance.get("executable_sha256") or digest == provenance["executable_sha256"]
        commands[name] = {"path": path, "sha256": digest, "provenance": provenance,
                          "status": "passed" if path and hash_matches else "failed"}
    libraries = {}
    for distribution, module in manifest["python_modules"].items():
        try:
            version = importlib.metadata.version(distribution)
            found = importlib.util.find_spec(module) is not None
            libraries[distribution] = {"module": module, "version": version,
                                       "status": "passed" if found else "failed"}
        except (ImportError, importlib.metadata.PackageNotFoundError) as error:
            libraries[distribution] = {"module": module, "status": "failed", "error": str(error)}
    # Query every apt package explicitly rather than truncating a bulk listing.
    package_checks = {}
    for name in manifest.get("apt_packages", []):
        result = execute(["dpkg-query", "-W", "-f=${Version}\t${Status}", name], check=False)
        resolved_name = name
        if result["returncode"] or "install ok installed" not in result["stdout"]:
            # apt accepts virtual aliases such as exiftool; the installed
            # executable belongs to libimage-exiftool-perl. Query the actual
            # package found from dpkg ownership at image construction.
            provider = manifest.get("command_provenance", {}).get(name, {}).get("package")
            if provider and provider != name:
                result = execute(["dpkg-query", "-W", "-f=${Version}\t${Status}", provider], check=False)
                resolved_name = provider
        package_checks[name] = {"status": "passed" if result["returncode"] == 0 and "install ok installed" in result["stdout"] else "failed",
                                "version": result["stdout"].split("\t")[0], "resolved_package": resolved_name}
    return {"commands": commands, "python_modules": libraries,
            "apt_packages": package_checks, "source_pins": manifest.get("sources", {}),
            "architecture_exclusions": manifest.get("architecture_exclusions", {}),
            "build_locks": manifest.get("build_locks", {}),
            "ruby_archives": manifest.get("ruby_archives", {}),
            "tools_documentation": {"path": str(tools_path), "sha256": tools_digest,
                                    "status": "passed" if tools_documentation_ok else "failed",
                                    "error": documentation_error,
                                    "expected_sha256": manifest.get("tools_txt_sha256"),
                                    "upstream_sha256": manifest.get("upstream_tools_sha256"),
                                    "environment_patches": manifest.get("tools_environment_patches", [])},
            "status": "passed" if tools_documentation_ok and commands and libraries and all(item["status"] == "passed" for group in (commands, libraries, package_checks) for item in group.values()) else "failed"}


def environment_denial(result):
    output = (result.get("stdout", "") + result.get("stderr", "")).lower()
    return any(text in output for text in ("operation not permitted", "permission denied", "cannot clone",
        "user namespaces are not enabled", "fuse: device not found", "/dev/fuse: no such file"))


def fuse_overlay_probe():
    with tempfile.TemporaryDirectory(prefix="fuse-acceptance-", dir=os.getcwd()) as directory:
        root = Path(directory)
        for name in ("lower", "upper", "work", "merged"):
            (root / name).mkdir()
        (root / "lower/base.txt").write_text("OFFLINE_LOWER_LAYER")
        options = ",".join(f"{name}dir={root/name}" for name in ("lower", "upper", "work"))
        result = execute(["fuse-overlayfs", "-o", options, root / "merged"], timeout=15, check=False)
        if result["returncode"]:
            return {"status": "environment_restricted" if environment_denial(result) else "failed",
                    "scope": "Actual overlay mount attempted with synthetic lower/upper/work/merged directories", **result}
        try:
            check((root / "merged/base.txt").read_text() == "OFFLINE_LOWER_LAYER", "FUSE lower layer read")
            (root / "merged/copied.txt").write_text("OFFLINE_UPPER_LAYER")
            check((root / "upper/copied.txt").read_text() == "OFFLINE_UPPER_LAYER", "FUSE upper layer write")
            return {"status": "passed", "verified": ["overlay mount", "lower read", "upper write"], **result}
        finally:
            execute(["fusermount3", "-u", root / "merged"], timeout=10, check=False)


def slirp_network_probe():
    # Attempt the precise kernel prerequisite even when /dev or container
    # permissions suggest it will fail; keep the real denial as evidence.
    prerequisite = execute(["unshare", "--user", "--map-root-user", "--net", "true"], timeout=10, check=False)
    if prerequisite["returncode"]:
        return {"status": "environment_restricted" if environment_denial(prerequisite) else "failed",
                "reason": "Creating the isolated user/network namespace required by slirp4netns was denied; no network setup is claimed.",
                "namespace_prerequisite": prerequisite}
    import select
    namespace = subprocess.Popen(["unshare", "--user", "--map-root-user", "--net", "sleep", "30"],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    ready_read, ready_write = os.pipe()
    exit_read, exit_write = os.pipe()
    slirp = None
    try:
        slirp = subprocess.Popen(["slirp4netns", "--configure", "--disable-host-loopback",
             "--ready-fd", str(ready_write), "--exit-fd", str(exit_read), str(namespace.pid), "tap0"],
             pass_fds=(ready_write, exit_read), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
             text=True, start_new_session=True)
        os.close(ready_write)
        ready_write = None
        readable, _, _ = select.select([ready_read], [], [], 10)
        if readable and os.read(ready_read, 1):
            verified = execute(["nsenter", "--target", str(namespace.pid), "--user", "--net", sys.executable,
                 "-c", "import socket; assert 'tap0' in dict((name,index) for index,name in socket.if_nameindex()); print('SLIRP_TAP_READY')"], timeout=10, check=False)
            return {"status": "passed" if verified["returncode"] == 0 else "environment_restricted" if environment_denial(verified) else "failed",
                    "namespace_prerequisite": prerequisite, "tap_probe": verified}
        if slirp.poll() is None:
            os.killpg(slirp.pid, signal.SIGTERM)
        stdout, stderr = slirp.communicate(timeout=3)
        result = {"returncode": slirp.returncode, "stdout": stdout, "stderr": stderr}
        return {"status": "environment_restricted" if environment_denial(result) else "failed",
                "namespace_prerequisite": prerequisite, **result}
    finally:
        for child in (slirp, namespace):
            if child is not None and child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()
        for descriptor in (ready_read, ready_write, exit_read, exit_write):
            if descriptor is not None:
                os.close(descriptor)


def nested_container_probe():
    components = {}
    with tempfile.TemporaryDirectory(prefix="compose-acceptance-", dir=os.getcwd()) as directory:
        compose = Path(directory, "compose.yml")
        compose.write_text('services:\n  fixture:\n    image: localhost/veria-offline-fixture:never-pulled\n    network_mode: none\n')
        result = execute(["podman-compose", "-f", compose, "config"], timeout=30, check=False)
        components["podman-compose"] = {"status": "passed" if result["returncode"] == 0 and "fixture" in result["stdout"] else "failed", **result}
    buildah = execute(["buildah", "info"], timeout=30, check=False)
    denial = re.search(r"operation not permitted|permission denied|unshare.*not permitted|cannot clone", (buildah["stdout"] + buildah["stderr"]).lower())
    components["buildah"] = {"status": "passed" if buildah["returncode"] == 0 else "environment_restricted" if denial else "failed",
                             "scope": "storage/kernel info only; image build not claimed", **buildah}
    for command, probe in (("fuse-overlayfs", fuse_overlay_probe), ("slirp4netns", slirp_network_probe)):
        components[command] = probe()
        components[command]["version_probe"] = execute([command, "--version"], timeout=10, check=False)
    def finish(item):
        if any(component["status"] == "failed" for component in components.values()):
            item["status"] = "failed"
        elif item["status"] == "passed" and any(component["status"] == "environment_restricted" for component in components.values()):
            item["status"] = "environment_restricted"
        item["components"] = components
        return item
    if not shutil.which("podman"):
        return finish({"status": "failed", "reason": "podman executable missing"})
    result = execute(["podman", "info", "--format", "json"], timeout=30, check=False)
    if result["returncode"] == 0:
        # A wholly local scratch image proves container execution without pulling
        # from a registry or touching benchmark targets. Use static libc so the
        # nested rootfs contains only our synthetic executable.
        import tarfile
        with tempfile.TemporaryDirectory(prefix="nested-acceptance-", dir=os.getcwd()) as directory:
            source = Path(directory, "hello.c")
            source.write_text('#include <stdio.h>\nint main(){puts("NESTED_RUN_OK");return 0;}\n')
            binary = Path(directory, "hello")
            execute(["gcc", "-static", "-Os", source, "-o", binary])
            archive = Path(directory, "rootfs.tar")
            with tarfile.open(archive, "w") as rootfs:
                rootfs.add(binary, arcname="hello")
            tag = f"localhost/veria-offline-acceptance:{os.getpid()}"
            imported = execute(["podman", "import", archive, tag], timeout=30, check=False)
            if imported["returncode"] == 0:
                try:
                    result = execute(["podman", "run", "--rm", "--pull=never", "--network=none", tag, "/hello"], timeout=30, check=False)
                    if result["returncode"] == 0 and "NESTED_RUN_OK" in result["stdout"]:
                        components["podman"] = {"status": "passed", "container_run": "passed", **result}
                        return finish({"status": "passed", "container_run": "passed", **result})
                finally:
                    execute(["podman", "image", "rm", "-f", tag], check=False)
            else:
                result = imported
    output = (result["stdout"] + result["stderr"]).lower()
    restricted = any(text in output for text in ("operation not permitted", "permission denied", "cannot clone", "cannot re-exec", "user namespaces are not enabled"))
    components["podman"] = {"status": "environment_restricted" if restricted else "failed", **result}
    return finish({"status": "environment_restricted" if restricted else "failed", "container_run": "not_passed", **result})


def verify(manifest_path=MANIFEST, phase="all", expect_uid=None):
    started = time.monotonic()
    network = network_state()
    report = {"kind": "veria_full_tool_acceptance", "model_calls": 0, "platform_calls": 0,
              "architecture": platform.machine(), "uid": os.getuid(), "python": sys.executable,
              "member": os.environ.get("XINGHUO_MEMBER"), "cwd": os.getcwd(),
              "manifest": str(manifest_path), "network_interfaces": sorted(network["interfaces"]), "network_state": network,
              "environment": {key: os.environ.get(key) for key in ("GHIDRA_INSTALL_DIR", "JAVA_HOME", "KERAS_BACKEND", "GEM_PATH")}}
    check(sys.platform == "linux", "Acceptance must run inside the Linux image")
    check(os.environ.get("XINGHUO_MEMBER"), "Acceptance must run through an actual member entrypoint")
    check(expect_uid is None or os.getuid() == expect_uid, "Runtime UID differs from requested UID")
    check("lo" in network["interfaces"] and not network["active_non_loopback"] and not network["errors"],
          "Run acceptance in docker --network none (only loopback may be active)")
    try:
        manifest = json.loads(manifest_path.read_text())
        check(isinstance(manifest, dict) and isinstance(manifest.get("commands"), list)
              and isinstance(manifest.get("python_modules"), dict), "Invalid tool manifest schema")
    except (OSError, ValueError, AssertionError) as error:
        report.update({"status": "failed", "manifest_error": str(error),
                       "seconds": round(time.monotonic() - started, 3)})
        return report
    if phase in {"inventory", "all"}:
        report["inventory"] = inventory(manifest)
    if phase in {"functional", "all"}:
        cases = {}
        for name in CASES:
            with tempfile.TemporaryDirectory(prefix="acceptance-" + name + "-", dir=os.getcwd()) as directory:
                result = execute([sys.executable, str(Path(__file__).resolve()), "--case", name, "--case-directory", directory],
                                 timeout=CASE_TIMEOUTS[name], check=False)
            details = None
            if result["returncode"] == 0:
                try:
                    details = json.loads(result["stdout"].strip().splitlines()[-1])
                except (json.JSONDecodeError, IndexError):
                    result["returncode"] = 1
                    result["stderr"] += "\nCase did not emit its structured result."
            status = "failed" if result["returncode"] else details.get("status", "passed")
            check(status in {"passed", "failed", "environment_restricted"}, "Unknown functional status")
            cases[name] = {"status": status, "details": details, **result}
        report["functional"] = cases
        try:
            report["nested_containers"] = nested_container_probe()
        except (OSError, RuntimeError) as error:
            report["nested_containers"] = {"status": "failed", "error": str(error)}
    failed = report.get("inventory", {}).get("status") == "failed" or any(result["status"] == "failed" for result in report.get("functional", {}).values()) or report.get("nested_containers", {}).get("status") == "failed"
    restricted = report.get("nested_containers", {}).get("status") == "environment_restricted" or any(
        item["status"] == "environment_restricted" for item in report.get("functional", {}).values())
    report["status"] = "failed" if failed else "passed_with_restrictions" if restricted else "passed"
    report["tool_results"] = per_tool_results(manifest, report)
    report["seconds"] = round(time.monotonic() - started, 3)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--phase", choices=("inventory", "functional", "all"), default="all")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--expect-uid", type=int)
    parser.add_argument("--case", choices=sorted(CASES))
    parser.add_argument("--case-directory", type=Path)
    args = parser.parse_args()
    if args.case:
        check(args.case_directory is not None, "case directory required")
        os.chdir(args.case_directory)
        print(json.dumps(CASES[args.case](), ensure_ascii=False))
        return 0
    result = verify(args.manifest, args.phase, args.expect_uid)
    content = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(content)
    print(content)
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
