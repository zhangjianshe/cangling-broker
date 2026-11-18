#!/usr/bin/env python3
"""Submit immutable Harbor images to the internal release worker and wait for the result."""
import argparse
import hashlib
import hmac
import json
import os
import re
import time
import urllib.error
import urllib.request

REGISTRY = 'harbor.cangling.cn:22002/cangling/'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request(base, secret, hook, payload):
    payload = dict(payload, timestamp=int(time.time()))
    body = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    signature = 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    message = urllib.request.Request(base.rstrip('/') + '/hooks/' + hook, data=body, headers={
        'Content-Type': 'application/json', 'X-Hub-Signature-256': signature,
    })
    with urllib.request.build_opener(NoRedirect()).open(message, timeout=40) as response:
        result = json.load(response)
    if 'error' in result and 'id' not in result:
        raise RuntimeError(result['error'])
    return result


def submit_and_wait(base, secret, payload, timeout=3600, poll=5, submit_only=False):
    job = request(base, secret, 'release-images', payload)
    print('Release task: ' + job['id'], flush=True)
    if submit_only:
        return job
    deadline = time.monotonic() + timeout
    previous = None
    errors = 0
    while True:
        state = (job['phase'], job.get('bytes_done'), job.get('bytes_total'), job.get('message'))
        if state != previous:
            print(job['phase'] + ': ' + job.get('message', '') + (' (' + str(job['bytes_done']) + '/' + str(job['bytes_total']) + ' bytes)' if job.get('bytes_total') else ''), flush=True)
            previous = state
        if job['status'] == 'succeeded':
            for artifact in job['result']['artifacts']:
                print(artifact['arch'] + ' ' + artifact['name'] + ' sha256=' + artifact['sha256'], flush=True)
            return job
        if job['status'] == 'failed':
            raise RuntimeError(job.get('error') or 'Release task failed')
        if time.monotonic() >= deadline:
            raise TimeoutError('Timed out waiting for release task ' + job['id'] + '; the server continues processing it')
        time.sleep(poll)
        try:
            job = request(base, secret, 'release-status', {'taskId': job['id']})
            errors = 0
        except (urllib.error.URLError, TimeoutError):
            errors += 1
            if errors > 5:
                raise
            print('Status temporarily unavailable; retrying', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True, choices=['cangling-broker', 'cangling-map', 'cangling-tile'])
    parser.add_argument('--version', required=True)
    parser.add_argument('--digest', required=True)
    parser.add_argument('--arch', action='append', choices=['amd64', 'arm64'])
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--allow-downgrade', action='store_true')
    parser.add_argument('--submit-only', action='store_true')
    parser.add_argument('--timeout', type=int, default=3600)
    args = parser.parse_args()
    if not re.fullmatch('sha256:[a-f0-9]{64}', args.digest):
        parser.error('Expected an immutable sha256 image digest')
    base = os.environ.get('RELEASE_WEBHOOK_URL', 'https://webhook.cangling.cn:22002')
    if not (base.startswith('https://') or base.startswith('http://127.0.0.1:')):
        raise ValueError('Webhook requires HTTPS')
    secret = os.environ.get('RELEASE_WEBHOOK_SECRET', '').strip()
    if not secret:
        raise ValueError('RELEASE_WEBHOOK_SECRET is required')
    payload = {'project': args.project, 'version': args.version,
               'images': {arch: REGISTRY + args.project + '@' + args.digest for arch in (args.arch or ['amd64', 'arm64'])},
               'dryRun': args.dry_run, 'allowDowngrade': args.allow_downgrade, 'retry': True}
    submit_and_wait(base, secret, payload, timeout=args.timeout, submit_only=args.submit_only)


if __name__ == '__main__':
    try: main()
    except Exception as error:
        raise SystemExit('Webhook release failed: ' + type(error).__name__ + ': ' + str(error))
