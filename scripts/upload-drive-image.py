#!/usr/bin/env python3
"""Publish offline image archives to cangling-repo/apps/base-images."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import time
import tarfile
import urllib.error
import urllib.parse
import urllib.request

ROOT = '05f8876b41974cfdadfa1f000d77809c'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Drive:
    def __init__(self, url, token, root=ROOT):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'https' and not (parsed.scheme == 'http' and parsed.hostname in ('localhost', '127.0.0.1')):
            raise ValueError('Drive URL must use HTTPS')
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('Invalid drive URL')
        if not token:
            raise ValueError('WRITE_TOKEN is required')
        self.base, self.token, self.root = url.rstrip('/') + '/api/v1/drive/', token, root
        self.client = urllib.request.build_opener(NoRedirect())

    def request(self, method, path, data=None, content_type='application/json', headers=None):
        body = json.dumps(data).encode() if isinstance(data, dict) else data
        request = urllib.request.Request(self.base + path, data=body, method=method,
                                         headers={**(headers or {}), 'X-Drive-Token': self.token, 'Content-Type': content_type})
        with self.client.open(request, timeout=180) as response:
            result = json.load(response)
        if result.get('code') != 200 or result.get('success') is False:
            raise RuntimeError('Drive API rejected ' + method + ' ' + path.split('?')[0])
        return result['data']

    def entries(self, parent):
        page = 1
        while True:
            result = self.request('GET', 'entries?' + urllib.parse.urlencode({'parentId': parent, 'page': page, 'pageSize': 1000}))
            yield from result['entries']
            if page * result['pageSize'] >= result['total']:
                break
            page += 1

    def directory(self, parent, name):
        entry = next((e for e in self.entries(parent) if e['name'] == name), None)
        if entry is None:
            try:
                entry = self.request('POST', 'directories', {'parentId': parent, 'name': name})
            except urllib.error.HTTPError as error:
                if error.code != 409:
                    raise
                entry = next(e for e in self.entries(parent) if e['name'] == name)
        if not entry['directory']:
            raise RuntimeError('Destination is not a directory: ' + name)
        return entry['id']


    def upload_file(self, parent, name, source):
        source = Path(source)
        with source.open('rb') as stream:
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        digest = digest.hexdigest()
        size = source.stat().st_size
        if size == 0:
            raise ValueError('Refusing empty archive: ' + str(source))
        existing = next((e for e in self.entries(parent) if e['name'] == name), None)
        if existing:
            if existing['directory']:
                raise ValueError('Refusing to replace a directory: ' + name)
            if existing.get('sha256') == digest and existing['size'] == size:
                print('Verified existing ' + name, flush=True)
                return digest
            self.request('DELETE', 'entries/' + existing['id'])
        # Stable ID lets a later invocation resume the same content after interruption.
        id = hashlib.sha256((parent + '/' + name + '/' + digest).encode()).hexdigest()[:32]
        status = None
        try:
            status = self.request('GET', 'uploads/' + id)
        except urllib.error.HTTPError as error:
            if error.code not in (404, 410):
                raise
            if error.code == 410:
                self.request('DELETE', 'uploads/' + id)
            else:
                # A previously published entry may have been deleted by a newer build.
                # Its old completed session must not be reused.
                try:
                    self.request('DELETE', 'uploads/' + id)
                except urllib.error.HTTPError as missing:
                    if missing.code != 404:
                        raise
        if status is None:
            status = self.request('POST', 'uploads', {'uploadId': id, 'parentId': parent, 'name': name, 'totalSize': size, 'sha256': digest})
        if status['sha256'] != digest or status['totalSize'] != size:
            raise RuntimeError('Upload session does not match local archive')
        with source.open('rb') as stream:
            failures = 0
            while not status['completed'] and status['receivedSize'] < size:
                offset = status['receivedSize']
                stream.seek(offset)
                chunk = stream.read(min(status['chunkSize'], size - offset))
                if not chunk:
                    raise RuntimeError('Local archive changed during upload')
                try:
                    status = self.request('PUT', 'uploads/' + id + '/chunk?' + urllib.parse.urlencode({'offset': offset, 'size': len(chunk)}), chunk, 'application/octet-stream', {'X-Chunk-SHA256': hashlib.sha256(chunk).hexdigest()})
                    failures = 0
                    print(name + ': ' + str(status['receivedSize']) + '/' + str(size) + ' bytes', flush=True)
                except (urllib.error.URLError, TimeoutError, OSError):
                    failures += 1
                    if failures > 3:
                        raise
                    time.sleep(failures)
                    # Server offset is authoritative if a response was lost after fsync.
                    status = self.request('GET', 'uploads/' + id)
        if not status['completed']:
            for attempt in range(3):
                try:
                    status = self.request('POST', 'uploads/' + id + '/complete')
                    break
                except (urllib.error.URLError, TimeoutError, OSError):
                    if attempt == 2:
                        raise
                    time.sleep(attempt + 1)
        entry = status.get('entry', {})
        if not status.get('completed') or entry.get('sha256') != digest or entry.get('size') != size:
            raise RuntimeError('Published archive checksum mismatch')
        print('Uploaded and SHA-256 verified ' + name, flush=True)
        return digest



def validate_archive(source, arch):
    # Validate actual saved image contents before touching the published file.
    with tarfile.open(source, 'r:gz') as archive:
        manifest = json.load(archive.extractfile('manifest.json'))
        if len(manifest) != 1:
            raise ValueError('Expected a single image in the archive')
        config = json.load(archive.extractfile(manifest[0]['Config']))
        if config.get('os') != 'linux' or config.get('architecture') != arch:
            raise ValueError('Archive image architecture does not match ' + arch)


def publish(drive, project, arch, source):
    if project not in ('cangling-broker', 'cangling-map', 'cangling-tile') or arch not in ('amd64', 'arm64'):
        raise ValueError('Unsupported project or architecture')
    source = Path(source)
    if not source.is_file() or source.stat().st_size == 0:
        raise ValueError('Missing or empty image archive')
    validate_archive(source, arch)
    parent = drive.root
    for part in ('apps', 'base-images', 'linux', arch):
        parent = drive.directory(parent, part)
    name = project + '-linux-' + arch + '.tar.gz'
    digest = drive.upload_file(parent, name, source)
    # Per-project checksums do not overwrite checksums for other images.
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        checksum = Path(tmp) / (name + '.sha256')
        checksum.write_text(digest + '  ' + name + '\n')
        drive.upload_file(parent, checksum.name, checksum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True, choices=['cangling-broker', 'cangling-map', 'cangling-tile'])
    parser.add_argument('--arch', required=True, choices=['amd64', 'arm64'])
    parser.add_argument('--file', required=True)
    args = parser.parse_args()
    drive = Drive(os.environ.get('CANGLING_DRIVE_URL', 'https://soft.cangling.cn:22002'), os.environ.get('WRITE_TOKEN', ''))
    publish(drive, args.project, args.arch, args.file)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit('Drive image upload failed: ' + type(error).__name__)
