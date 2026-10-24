#!/usr/bin/env python3
"""Record the complete upstream installation contract; executed at image build."""
import importlib.metadata
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess


COMMANDS = """bash python python3 pip pip3 nc curl wget nmap file strings xxd
hexdump binwalk objdump readelf nm gdb ltrace strace r2 radare2 steghide stegseek
zsteg exiftool pngcheck identify convert montage tesseract ffmpeg ffprobe sox
mmls fls icat tsk_recover fsstat foremost testdisk xfs_db xfs_repair mkfs.xfs
dcfldd vol openssl RsaCtfTool sage cado-nfs flatter gcc g++ make cmake pkg-config
jq zip unzip git podman podman-compose buildah fuse-overlayfs slirp4netns uv uvx
ruby gem java javac ROPgadget mkfs.ext2 debugfs""".split()
PYTHON_MODULES = {
    "pwntools": "pwn", "pycryptodome": "Crypto", "sympy": "sympy",
    "gmpy2": "gmpy2", "requests": "requests", "Pillow": "PIL",
    "z3-solver": "z3", "pytesseract": "pytesseract", "scipy": "scipy",
    "numpy": "numpy", "angr": "angr", "capstone": "capstone",
    "unicorn": "unicorn", "ROPGadget": "ropgadget", "tqdm": "tqdm",
    "fpylll": "fpylll", "cysignals": "cysignals", "Flask": "flask",
    "volatility3": "volatility3", "PyJWT": "jwt", "pyghidra": "pyghidra",
    "torch": "torch", "keras": "keras", "podman-compose": "podman_compose",
    "RsaCtfTool": "RsaCtfTool",
    "pip": "pip", "setuptools": "setuptools", "wheel": "wheel",
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def command_provenance(command, path, sources, apt_versions):
    if not path:
        return {"status": "missing"}
    record = {"executable_sha256": sha256(path)}
    pins = {"r2": "radare2", "radare2": "radare2", "flatter": "flatter",
            "stegseek": "stegseek", "RsaCtfTool": "rsactftool", "cado-nfs": "cado-nfs",
            "java": "java-" + ("arm64" if platform.machine() == "aarch64" else "amd64"),
            "javac": "java-" + ("arm64" if platform.machine() == "aarch64" else "amd64")}
    if command in pins:
        record.update({"source": pins[command], "version": sources[pins[command]].get("version"),
                       "commit": sources[pins[command]].get("commit")})
        return record
    if command in {"python", "python3", "uv", "uvx"}:
        record.update({"version": platform.python_version() if command.startswith("python") else "0.13.1",
                       "source": "uv-verified-python-distribution" if command.startswith("python") else "pinned-uv-image"})
        return record
    if command == "ROPgadget":
        record.update({"distribution": "ROPGadget", "version": importlib.metadata.version("ROPGadget"),
                       "source": "hashed-python-requirements-lock"})
        return record
    for entry in importlib.metadata.entry_points(group="console_scripts"):
        if entry.name == command and entry.dist:
            record.update({"distribution": entry.dist.metadata["Name"], "version": entry.dist.version,
                           "source": "hashed-python-requirements-lock"})
            return record
    candidates = [path, str(Path(path).resolve())]
    # Ubuntu's merged /usr layout exposes /usr/bin and /usr/sbin even when
    # dpkg records the historic /bin and /sbin ownership paths.
    candidates += [candidate[4:] for candidate in candidates if candidate.startswith(("/usr/bin/", "/usr/sbin/"))]
    for candidate in dict.fromkeys(candidates):
        result = subprocess.run(["dpkg-query", "-S", candidate], capture_output=True, text=True)
        if result.returncode == 0:
            package = result.stdout.split(": ", 1)[0]
            record.update({"package": package, "version": apt_versions.get(package, apt_versions.get(package.split(":")[0])),
                           "source": "apt-artifacts.lock.json"})
            return record
    if command == "zsteg":
        record.update({"version": "0.2.13", "source": "ruby-gem-archive-sha256"})
    return record


def main():
    root = Path("/opt/xinghuo")
    build = root / "build"
    gem_path = subprocess.check_output(["ruby", "-e", 'require "rubygems"; puts Gem.path.join(":")'], text=True).strip()
    sources = json.loads((build / "sources.lock.json").read_text())
    apt = [line.strip() for line in (build / "apt-packages.txt").read_text().splitlines()
           if line.strip() and not line.startswith("#")]
    apt_versions = dict(line.split("=", 1) for line in (build / "apt-installed.lock").read_text().splitlines())
    manifest = {"format": 2, "upstream_commit": "3366d569557c4fda3fd153040632de65e255396d",
        "architecture": platform.machine(), "tool_python": "/opt/ctf/bin/python3",
        "sdk_python": "/opt/sdk/bin/python", "commands": COMMANDS,
        "python_modules": PYTHON_MODULES, "apt_packages": apt, "sources": sources,
        "environment": {"GHIDRA_INSTALL_DIR": "/opt/ghidra", "JAVA_HOME": "/opt/java",
                        "KERAS_BACKEND": "torch", "GEM_PATH": gem_path,
                        "UV_PYTHON_INSTALL_DIR": "/opt/python"},
        "architecture_exclusions": {},
        "installed_commands": {command: shutil.which(command) for command in COMMANDS},
        "installed_python_versions": {distribution: importlib.metadata.version(distribution)
                                      for distribution in PYTHON_MODULES},
        "runtime_caveats": ["Podman installation does not prove nested-container permission.",
                            "Hosted networking has no public Internet; platform grants apply.",
                            "Ghidra project_location must point to a member-writable directory."]}
    missing = [name for name, path in manifest["installed_commands"].items() if not path]
    if missing:
        raise SystemExit("Missing required upstream commands: " + ", ".join(missing))
    manifest["command_provenance"] = {command: command_provenance(command, path, sources, apt_versions)
                                      for command, path in manifest["installed_commands"].items()}
    manifest["build_locks"] = {path.name: sha256(path) for path in build.iterdir()
                                if path.is_file() and ("lock" in path.name or path.name.endswith(".in"))}
    manifest["ruby_archives"] = {str(path): sha256(path) for directory in gem_path.split(":")
                                  for path in Path(directory, "cache").glob("*.gem")}
    text = (build / "upstream-tools.txt").read_text()
    manifest["upstream_tools_sha256"] = hashlib.sha256(text.encode()).hexdigest()
    replacements = {
        "- Sandbox runs ARM64. Do NOT execute x86/x86-64 binaries locally.\n  Use Ghidra tools for RE, and interact with remote services for dynamic analysis.":
            f"- Sandbox architecture: {platform.machine()}. Execute only compatible native binaries locally.\n  Use Ghidra tools for RE, and interact with authorized services for other architectures.",
        "- Need a package not listed? Run `pip install <pkg>` or `uv pip install <pkg>`.":
            "- Need a package not listed? In a local environment with Internet access, run `pip install <pkg>` or `uv pip install <pkg>`. Hosted environments have no public Internet; installation requires an available local artifact.",
        "- Distfiles are at /challenge/distfiles/ (read-only)":
            "- Distfiles are under the attempt inputs directory (read-only); use the exact path in your system prompt.",
        "- Write generated/repaired files to /challenge/workspace/ (writable)":
            "- Write generated/repaired files to your member work directory (the current shell directory). Use the exact shared directory from your system prompt for handoffs.",
        "- Services on the host are reachable via host.docker.internal (bridge networking).":
            "- Local Docker bridge networking can reach host services via host.docker.internal. Hosted execution uses platform networking and the supplied service addresses unchanged.",
    }
    for before, after in replacements.items():
        if text.count(before) != 1:
            raise SystemExit("Upstream tools text changed; review environment patch: " + before)
        text = text.replace(before, after)
    text += "\n== ADDITIONAL ENVIRONMENT DETAILS ==\n"
    text += "- pyghidra is installed with Ghidra and a compatible JDK. Use a member-writable project_location when opening read-only input files.\n"
    text += "- Keras uses the installed CPU PyTorch backend.\n"
    text += "- Podman/buildah are installed; nested containers require host kernel permissions. Installation alone is not a successful runtime test.\n"
    text += "- Native CTF Python: /opt/ctf/bin/python3. SDK Python is separate at /opt/sdk/bin/python. Member Python environments inherit CTF libraries.\n"
    Path("/tools.txt").write_text(text)
    manifest["tools_txt_sha256"] = hashlib.sha256(text.encode()).hexdigest()
    manifest["tools_environment_patches"] = [{"upstream": before, "replacement": after}
                                               for before, after in replacements.items()]
    (root / "tool-manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
