import hashlib
import hmac
import importlib.util
import io
import json
import os
import sys
from pathlib import Path
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('client', Path(__file__).with_name('release-via-webhook.py'))
client = importlib.util.module_from_spec(spec); spec.loader.exec_module(client)


class ClientTests(unittest.TestCase):
    def test_signed_request_and_nullable_job_error(self):
        captured = []
        class Opener:
            def open(self, request, timeout):
                captured.append(request)
                return io.BytesIO(json.dumps({'id':'a'*64,'status':'queued','error':None}).encode())
        with patch.object(client.urllib.request,'build_opener',return_value=Opener()):
            job=client.request('https://example.com','test-key','release-images',{'project':'cangling-map'})
        request=captured[0]; self.assertEqual(job['status'],'queued')
        self.assertEqual(request.full_url,'https://example.com/hooks/release-images')
        expected='sha256='+hmac.new(b'test-key',request.data,hashlib.sha256).hexdigest()
        self.assertEqual(request.get_header('X-hub-signature-256'),expected)
        self.assertLess(abs(json.loads(request.data)['timestamp']-time.time()),3)

    def test_waits_for_server_owned_job_and_returns_artifacts(self):
        states=[{'id':'a'*64,'phase':'queued','status':'queued','error':None},
                {'id':'a'*64,'phase':'pull','status':'running','message':'pulling'},
                {'id':'a'*64,'phase':'complete','status':'succeeded','result':{'artifacts':[{'arch':'amd64','name':'image.tar.gz','sha256':'b'*64}]}}]
        with patch.object(client,'request',side_effect=states) as request, patch.object(client.time,'sleep'):
            job=client.submit_and_wait('https://example.com','key',{},timeout=10)
        self.assertEqual(job['status'],'succeeded')
        self.assertEqual([call.args[2] for call in request.call_args_list],['release-images','release-status','release-status'])

    def test_failed_server_task_fails_actions(self):
        job={'id':'a'*64,'phase':'failed','status':'failed','error':'packing failed'}
        with patch.object(client,'request',return_value=job):
            with self.assertRaisesRegex(RuntimeError,'packing failed'):client.submit_and_wait('https://example.com','key',{})

    def test_submit_only_returns_without_polling(self):
        job={'id':'a'*64,'phase':'queued','status':'queued'}
        with patch.object(client,'request',return_value=job) as request:
            self.assertEqual(client.submit_and_wait('https://example.com','key',{},submit_only=True),job)
            request.assert_called_once()

    def test_environment_secret_tolerates_file_newline(self):
        argv=['release-via-webhook.py','--project','cangling-map','--version','0.1.27','--digest','sha256:'+'a'*64]
        with patch.object(sys,'argv',argv),patch.dict(os.environ,{'RELEASE_WEBHOOK_SECRET':'k'*64+'\n','RELEASE_WEBHOOK_URL':'https://example.com'},clear=True),patch.object(client,'submit_and_wait') as submit:
            client.main()
        self.assertEqual(submit.call_args.args[1],'k'*64)

    def test_redirects_are_not_followed(self):
        self.assertIsNone(client.NoRedirect().redirect_request(None,None,302,'',{},'https://other.example'))


if __name__=='__main__':unittest.main()
