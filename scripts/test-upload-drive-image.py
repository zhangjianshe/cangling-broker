import hashlib
import io
import json
import tarfile
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

spec = importlib.util.spec_from_file_location('upload', Path(__file__).with_name('upload-drive-image.py'))
upload = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upload)


def write_image(path, arch):
    with tarfile.open(path, 'w:gz') as archive:
        for name, data in [('manifest.json', [{'Config': 'config.json', 'RepoTags': ['test:1.2.3'], 'Layers': []}]), ('config.json', {'architecture': arch, 'os': 'linux'})]:
            body = json.dumps(data).encode()
            entry = tarfile.TarInfo(name); entry.size = len(body)
            archive.addfile(entry, io.BytesIO(body))



class MemoryDrive(upload.Drive):
    def __init__(self):
        super().__init__('https://example.com', 'test-only')
        self.children = {self.root: []}
        self.sessions = {}
        self.chunks = []
        self.lost_response = False
        self.fail_complete = False
        self.deletions = []

    def entries(self, parent):
        return iter(self.children[parent])

    def request(self, method, path, data=None, content_type='application/json', headers=None):
        if path == 'directories':
            entry = {'name': data['name'], 'directory': True, 'id': 'dir-' + str(len(self.children))}
            self.children[data['parentId']].append(entry)
            self.children[entry['id']] = []
            return entry
        if path == 'uploads':
            status = dict(data, receivedSize=0, completed=False, chunkSize=4)
            self.sessions[data['uploadId']] = (status, bytearray())
            return status.copy()
        if path.startswith('entries/'):
            id = path.split('/')[1]
            self.deletions.append(id)
            for group in self.children.values():
                group[:] = [e for e in group if e['id'] != id]
            return {}
        parts = path.split('?')[0].split('/')
        id = parts[1]
        if id not in self.sessions:
            raise urllib.error.HTTPError('https://example.com', 404, 'missing', {}, None)
        status, body = self.sessions[id]
        if method == 'GET':
            return status.copy()
        if method == 'DELETE':
            del self.sessions[id]
            return {}
        if parts[-1] == 'chunk':
            query = urllib.parse.parse_qs(path.split('?')[1])
            assert int(query['offset'][0]) == len(body)
            assert hashlib.sha256(data).hexdigest() == headers['X-Chunk-SHA256']
            self.chunks.append(len(body))
            body.extend(data)
            status['receivedSize'] = len(body)
            if self.lost_response:
                self.lost_response = False
                raise TimeoutError('response lost after the server accepted the chunk')
            return status.copy()
        if parts[-1] == 'complete':
            if self.fail_complete:
                raise RuntimeError('completion failed')
            assert hashlib.sha256(body).hexdigest() == status['sha256']
            entry = {'id': 'file-' + id, 'name': status['name'], 'directory': False, 'sha256': status['sha256'], 'size': len(body)}
            self.children[status['parentId']].append(entry)
            status.update(completed=True, entry=entry)
            return status.copy()
        raise AssertionError((method, path))


class UploadTests(unittest.TestCase):
    def test_stable_layout_and_checksum_for_all_projects_and_architectures(self):
        for project in ['cangling-broker', 'cangling-map', 'cangling-tile']:
            for arch in ['amd64', 'arm64']:
                with tempfile.TemporaryDirectory() as tmp:
                    source = Path(tmp) / (project + '-v1.2.3.tar.gz')
                    write_image(source, arch)
                    drive = MemoryDrive()
                    upload.publish(drive, project, arch, source)
                    parent = drive.root
                    for part in ['apps', 'base-images', 'linux', arch]:
                        parent = next(e['id'] for e in drive.children[parent] if e['name'] == part)
                    expected = project + '-linux-' + arch + '.tar.gz'
                    self.assertEqual({e['name'] for e in drive.children[parent]}, {expected, expected + '.sha256'})
                    checksum_session = next((s, body) for s, body in drive.sessions.values() if s['name'].endswith('.sha256'))
                    self.assertEqual(checksum_session[1].decode(), hashlib.sha256(source.read_bytes()).hexdigest() + '  ' + expected + '\n')

    def test_resume_after_lost_response_and_skip_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'archive'; source.write_bytes(b'0123456789')
            drive = MemoryDrive(); drive.lost_response = True
            with patch.object(upload.time, 'sleep'):
                drive.upload_file(drive.root, 'archive', source)
            self.assertEqual(drive.chunks, [0, 4, 8])
            drive.upload_file(drive.root, 'archive', source)
            self.assertEqual(drive.chunks, [0, 4, 8])
            source.write_bytes(b'replacement')
            drive.upload_file(drive.root, 'archive', source)
            self.assertEqual(len(drive.deletions), 1)
            self.assertEqual(len(drive.children[drive.root]), 1)

    def test_failed_binary_does_not_publish_checksum(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'archive'; write_image(source, 'amd64')
            drive = MemoryDrive(); drive.fail_complete = True
            with self.assertRaises(RuntimeError):
                upload.publish(drive, 'cangling-map', 'amd64', source)
            self.assertFalse(any(s['name'].endswith('.sha256') for s, _ in drive.sessions.values()))

    def test_directory_collision_is_never_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'archive'; source.write_bytes(b'data')
            drive = MemoryDrive(); drive.directory(drive.root, 'archive')
            with self.assertRaises(ValueError):
                drive.upload_file(drive.root, 'archive', source)
            self.assertEqual(drive.deletions, [])

    def test_wrong_architecture_is_rejected_before_any_remote_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'archive'; write_image(source, 'amd64')
            drive = MemoryDrive()
            with self.assertRaises(ValueError):
                upload.publish(drive, 'cangling-tile', 'arm64', source)
            self.assertEqual(len(drive.children), 1)
            self.assertEqual(drive.sessions, {})
            self.assertEqual(drive.deletions, [])

    def test_https_and_redirect_safety(self):
        with self.assertRaises(ValueError):
            upload.Drive('http://example.com', 'key')
        self.assertIsNone(upload.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://other.example'))


if __name__ == '__main__':
    unittest.main()
