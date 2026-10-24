#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
hosted_tag="${XINGHUO_IMAGE_TAG:-xinghuo-hosted:flash4-idle15-prompts-20261017}"
tools_tag="${XINGHUO_TOOLS_IMAGE_TAG:-xinghuo-veria-tools:full-amd64-20261016}"
release_dir="${XINGHUO_RELEASE_DIR:-dist/hosted-flash4-idle15-prompts-20261017}"
reuse_tools="${XINGHUO_REUSE_TOOLS_IMAGE:-0}"
case "$reuse_tools" in
    0|1) ;;
    *) printf '%s\n' 'XINGHUO_REUSE_TOOLS_IMAGE must be 0 or 1.' >&2; exit 1 ;;
esac
mkdir -p "$release_dir"
archive_name=xinghuo-hosted-linux-amd64.tar.gz
# Never let an earlier package or acceptance report masquerade as this build.
for filename in "$archive_name" "$archive_name.sha256" archive-summary.json source-check.json container-check.json tool-acceptance.json pressure-check.json build-info.json; do
    if [[ -e "$release_dir/$filename" ]]; then
        printf 'Refusing existing release artifact: %s. Use a new XINGHUO_RELEASE_DIR.\n' "$release_dir/$filename" >&2
        exit 1
    fi
done
if ! mkdir "$release_dir/.build.lock"; then
    printf 'Another build owns %s/.build.lock; inspect that build before retrying.\n' "$release_dir" >&2
    exit 1
fi
stage_dir=""
cleanup() {
    status=$?
    if [[ "$status" -ne 0 && -f "$release_dir/archive-summary.json" ]]; then
        printf 'Build did not satisfy release requirements; inspect %s/archive-summary.json.\n' "$release_dir" >&2
    elif [[ "$status" -ne 0 && -n "$stage_dir" ]]; then
        printf 'Build failed; incomplete evidence retained at %s. No successful release summary was published.\n' "$stage_dir" >&2
    fi
    rmdir "$release_dir/.build.lock"
}
trap cleanup EXIT
stage_dir="$(mktemp -d "$release_dir/.build-XXXXXXXX")"
if [[ "$reuse_tools" == 1 ]]; then
    # Explicit reuse is for an already installed, previously accepted full tool
    # image. Inspect must succeed locally; this branch never silently pulls it.
    printf 'Reusing local complete tools image: %s\n' "$tools_tag"
else
    docker buildx build --platform linux/amd64 --provenance=false --load -f Dockerfile.veria-tools -t "$tools_tag" .
fi
tools_platform="$(docker image inspect "$tools_tag" --format '{{.Os}}/{{.Architecture}}')"
if [[ "$tools_platform" != linux/amd64 ]]; then
    printf 'Tools image must be linux/amd64; got %s.\n' "$tools_platform" >&2
    exit 1
fi
tools_image_id="$(docker image inspect "$tools_tag" --format '{{.Id}}')"
docker buildx build --platform linux/amd64 --provenance=false --load --build-arg "VERIA_TOOLS_IMAGE=$tools_tag" -f Dockerfile.hosted -t "$hosted_tag" .
hosted_platform="$(docker image inspect "$hosted_tag" --format '{{.Os}}/{{.Architecture}}')"
[[ "$hosted_platform" == linux/amd64 ]] || { printf 'Hosted image must be linux/amd64; got %s.\n' "$hosted_platform" >&2; exit 1; }
hosted_image_id="$(docker image inspect "$hosted_tag" --format '{{.Id}}')"
docker run --rm --network none "$hosted_tag" --check
.venv-veria/bin/python scripts/verify_veria_release_source.py --image "$hosted_tag" --output "$stage_dir/source-check.json"
docker run --rm --network none --mount "type=bind,src=$project_root/scripts/verify_hosted_image.py,dst=/tmp/verify_hosted_image.py,readonly" \
    --entrypoint /opt/sdk/bin/python "$hosted_tag" /tmp/verify_hosted_image.py > "$stage_dir/container-check.json"
.venv-veria/bin/python scripts/verify_full_veria_image.py --image "$hosted_tag" --mode hosted --output "$stage_dir/tool-acceptance.json"
.venv-veria/bin/python scripts/run_mixed4_pressure.py --image "$hosted_tag" --output "$stage_dir/pressure-check.json"
# Source may have changed during a lengthy acceptance run. Recheck before
# exporting so the published evidence never describes an earlier code snapshot.
.venv-veria/bin/python scripts/verify_veria_release_source.py --image "$hosted_tag" --output "$stage_dir/source-check.json"
[[ "$(docker image inspect "$tools_tag" --format '{{.Id}}')" == "$tools_image_id" ]] || { printf '%s\n' 'Tools image tag changed during acceptance.' >&2; exit 1; }
[[ "$(docker image inspect "$hosted_tag" --format '{{.Id}}')" == "$hosted_image_id" ]] || { printf '%s\n' 'Hosted image tag changed during acceptance.' >&2; exit 1; }
# Export the accepted tag and check its image identity again after export,
# before any final file is published.
docker save "$hosted_tag" | gzip -n > "$stage_dir/$archive_name.tmp"
gzip -t "$stage_dir/$archive_name.tmp"
[[ "$(docker image inspect "$hosted_tag" --format '{{.Id}}')" == "$hosted_image_id" ]] || { printf '%s\n' 'Hosted image tag changed during export.' >&2; exit 1; }
python3 - "$stage_dir" "$release_dir" "$archive_name" "$hosted_tag" "$hosted_image_id" "$tools_tag" "$tools_image_id" "$reuse_tools" <<'PY'
import datetime, hashlib, json, os, pathlib, sys
stage, release = map(pathlib.Path, sys.argv[1:3])
name, image, image_id, tools, tools_id, reused = sys.argv[3:]
path = stage / (name + '.tmp')
with path.open('rb') as stream:
    value = hashlib.file_digest(stream, 'sha256').hexdigest()
size = path.stat().st_size
build = {'built_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
         'image': image, 'image_id': image_id, 'platform': 'linux/amd64',
         'tools_image': tools, 'tools_image_id': tools_id, 'tools_image_reused': reused == '1'}
(stage / 'build-info.json').write_text(json.dumps(build, indent=2) + '\n')
(stage / (name + '.sha256')).write_text(f'{value}  {name}\n')
summary = {**build, 'archive': name, 'bytes': size, 'sha256': value,
           'upload_limit_bytes': 3_000_000_000, 'uploadable': size < 3_000_000_000,
           'platform_test': 'pending_user_upload', 'tools_removed_for_size': False}
(stage / 'archive-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
# Rename only complete files on the same filesystem. Publish the summary last:
# without it the release is incomplete, even if an interrupted rename left files.
os.replace(path, release / name)
for filename in (name + '.sha256', 'source-check.json', 'container-check.json',
                 'tool-acceptance.json', 'pressure-check.json', 'build-info.json', 'archive-summary.json'):
    os.replace(stage / filename, release / filename)
stage.rmdir()
print(json.dumps(summary))
if not summary['uploadable']:
    raise SystemExit('Complete archive exceeds 3GB: retained intact; not uploadable.')
PY
printf '%s\n' 'Prepared upload archive only. No model, task creation or platform submission performed.'
