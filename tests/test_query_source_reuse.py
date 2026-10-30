"""Direct public-window controls with only synthetic encrypted databases."""
import contextlib, io, json, os, shutil, sqlite3, unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch
from tests import test_codex_first_voice_cli as fixtures
GROUP=fixtures.GROUP
TABLE=fixtures.TABLE
_open_encrypted_writer=fixtures._open_encrypted_writer
import wechat_local.codex_first_runtime as runtime
import wechat_local.live_catalog as catalog
import wechat_local.live_readonly as readonly
import wechat_local.image_runtime as image_runtime

class QuerySourceReuseTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CodexFirstVoiceCliTests();self.fixture.setUp()
        self.f=self.fixture
        self.attachments=self.f.root/'attachments';self.attachments.mkdir()
        self.output=self.f.root/'images'

    def tearDown(self):self.fixture.tearDown()

    def add_roles(self,path,voice):
        c=_open_encrypted_writer(path)
        c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT,verify_flag INTEGER)')
        c.execute('INSERT INTO contact VALUES(?,?,?,?,?)',(GROUP,'','Synthetic name','',0))
        c.execute('CREATE TABLE ChatName2Id(rowid INTEGER PRIMARY KEY,user_name TEXT)')
        c.execute('CREATE TABLE MessageResourceInfo(chat_id INTEGER,message_svr_id INTEGER,message_local_id INTEGER,message_create_time INTEGER,message_local_type INTEGER,message_id TEXT,packed_info BLOB)')
        c.execute('CREATE TABLE MessageResourceDetail(message_id TEXT,size INTEGER)')
        c.execute('INSERT INTO ChatName2Id VALUES(1,?)',(GROUP,))
        c.execute('INSERT INTO MessageResourceInfo VALUES(1,103,3,1700000020,3,?,?)',('synthetic-resource',b'<img>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</img>'))
        c.execute('INSERT INTO MessageResourceDetail VALUES(?,4)',('synthetic-resource',))
        if voice:
            c.execute('CREATE TABLE VoiceInfo(chat_name_id INTEGER,create_time INTEGER,local_id INTEGER,svr_id INTEGER,voice_data BLOB,data_index BLOB)')
            c.execute('INSERT INTO VoiceInfo VALUES(7,1700000010,2,102,?,NULL)',(b'\x02#!SILK_V3'+b'x'*32,))
        c.commit();c.close()
        c=_open_encrypted_writer(self.f.message_path)
        c.execute(f'INSERT INTO "{TABLE}" VALUES(3,103,3,1700000020,9,?,0,NULL)',('synthetic image',))
        c.commit();c.close()

    def query(self,shared_path,mutate=False,voice_on=True,mutate_after_image=False,mutate_early=False,expect_exit=0):
        counts=Counter();raw_open=readonly.open_encrypted_readonly;raw_derive=readonly.derive_database_key;raw_verify=readonly.verify_encryption_key
        class Conn:
            def __init__(self,c,path):self.c=c;self.path=path
            def close(self):counts['close|'+str(self.path)]+=1;return self.c.close()
            def __getattr__(self,name):return getattr(self.c,name)
        def opened(path,*args,**kwargs):
            c,meta=raw_open(path,*args,**kwargs);counts['open|'+str(path.resolve())]+=1;return Conn(c,path),meta
        def derive(*args,**kwargs):counts['kdf']+=1;return raw_derive(*args,**kwargs)
        def verify(*args,**kwargs):counts['hmac']+=1;return raw_verify(*args,**kwargs)
        argv=['--live-db-root',str(self.f.live_root),'--message-root',str(self.f.message_root),'--passphrase',str(self.f.passphrase),
              '--event-store',str(self.f.event_store_path),'--agent-store',str(self.f.agent_store_path),'--account-id','synthetic-account',
              '--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC',
              '--encrypted-contact-database',str(shared_path),
              '--image-database',str(shared_path),'--image-attachments-root',str(self.attachments),'--image-output-root',str(self.output),'--json']
        if voice_on:argv+=['--voice-transcripts','--voice-database',str(shared_path)]
        output=io.StringIO();errors=io.StringIO()
        original_scan=runtime.CodexFirstRuntime.scan
        def scan(instance,*args,**kwargs):
            if mutate:
                replacement=shared_path.with_name('replacement.db')
                shutil.copyfile(shared_path,replacement);os.replace(replacement,shared_path)
            return original_scan(instance,*args,**kwargs)
        original_init=runtime.LiveIdentityAdapter.__init__
        def init(instance,*args,**kwargs):
            original_init(instance,*args,**kwargs)
            if mutate_early:
                replacement=shared_path.with_name('replacement.db')
                shutil.copyfile(shared_path,replacement);os.replace(replacement,shared_path)
        original_image=image_runtime.enrich_bound_images
        def image(*args,**kwargs):
            result=original_image(*args,**kwargs)
            if mutate_after_image:
                replacement=shared_path.with_name('replacement.db')
                shutil.copyfile(shared_path,replacement);os.replace(replacement,shared_path)
            return result
        with patch.object(runtime,'open_encrypted_readonly',side_effect=opened),patch.object(catalog,'open_encrypted_readonly',side_effect=opened),patch.object(readonly,'derive_database_key',side_effect=derive),patch.object(readonly,'verify_encryption_key',side_effect=verify),patch.object(runtime.CodexFirstRuntime,'scan',new=scan),patch.object(runtime.LiveIdentityAdapter,'__init__',new=init),patch.object(image_runtime,'enrich_bound_images',new=image),contextlib.redirect_stdout(output),contextlib.redirect_stderr(errors):
            code=runtime.main(argv)
        self.assertEqual(code,expect_exit,errors.getvalue());return json.loads(output.getvalue()),counts

    def test_message_contact_voice_image_same_source_opened_and_closed_once(self):
        path=self.f.message_path;self.add_roles(path,voice=True)
        result,counts=self.query(path)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['total_count'],3)
        self.assertEqual(result['contact_name_source'],'configured')
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['kdf'],1,counts);self.assertEqual(counts['hmac'],1,counts)

    def test_separate_voice_contact_image_source_opened_and_closed_once(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        result,counts=self.query(path)
        self.assertEqual(result['status'],'ok');self.assertEqual(result['total_count'],3)
        self.assertEqual(result['contact_name_source'],'configured')
        self.assertEqual(counts['open|'+str(self.f.message_path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(self.f.message_path.resolve())],1,counts)
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['kdf'],2,counts);self.assertEqual(counts['hmac'],2,counts)

    def test_shared_voice_contact_image_replacement_does_not_use_stale_source(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        result,counts=self.query(path,mutate=True)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['contact_name_source'],'unavailable')
        refs=[ref for group in result['groups'] for ref in group['event_refs']]
        self.assertTrue(any(ref.get('voice',{}).get('status')=='voice_source_unavailable' for ref in refs))
        self.assertTrue(any(ref.get('image',{}).get('reason')=='image_source_unavailable' for ref in refs))
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)

    def test_contact_and_image_same_separate_source_without_voice(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        result,counts=self.query(path,voice_on=False)
        self.assertEqual(result['status'],'ok');self.assertEqual(result['total_count'],3)
        self.assertEqual(result['contact_name_source'],'configured')
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['kdf'],2,counts);self.assertEqual(counts['hmac'],2,counts)

    def test_shared_contact_image_replacement_without_voice_is_unavailable(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        result,counts=self.query(path,mutate=True,voice_on=False)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['contact_name_source'],'unavailable')
        refs=[ref for group in result['groups'] for ref in group['event_refs']]
        self.assertTrue(any(ref.get('image',{}).get('reason')=='image_source_unavailable' for ref in refs))
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)

    def test_source_replaced_during_image_lookup_discards_image_result(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        result,counts=self.query(path,voice_on=False,mutate_after_image=True)
        refs=[ref for group in result['groups'] for ref in group['event_refs']]
        self.assertTrue(any(ref.get('image',{}).get('reason')=='image_source_unavailable' for ref in refs))
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)

    def test_shared_wrong_key_source_is_checked_only_once_and_unavailable(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        c=_open_encrypted_writer(path)
        c.execute("PRAGMA rekey = '"+'t'*32+"'");c.close()
        result,counts=self.query(path)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['contact_name_source'],'unavailable')
        refs=[ref for group in result['groups'] for ref in group['event_refs']]
        self.assertTrue(any(ref.get('voice',{}).get('status')=='voice_source_unavailable' for ref in refs))
        self.assertTrue(any(ref.get('image',{}).get('reason')=='image_source_unavailable' for ref in refs))
        self.assertEqual(counts['kdf'],2,counts);self.assertEqual(counts['hmac'],2,counts)

    def test_contact_image_wrong_key_source_is_checked_once_without_voice(self):
        path=self.f.voice_path;self.add_roles(path,voice=False)
        c=_open_encrypted_writer(path);c.execute("PRAGMA rekey = '"+'t'*32+"'");c.close()
        result,counts=self.query(path,voice_on=False)
        self.assertEqual(result['status'],'ok');self.assertEqual(result['contact_name_source'],'unavailable')
        refs=[ref for group in result['groups'] for ref in group['event_refs']]
        self.assertTrue(any(ref.get('image',{}).get('reason')=='image_source_unavailable' for ref in refs))
        self.assertEqual(counts['kdf'],2,counts);self.assertEqual(counts['hmac'],2,counts)

    def test_message_source_replaced_before_runtime_binding_is_unavailable(self):
        path=self.f.message_path;self.add_roles(path,voice=True)
        result,counts=self.query(path,mutate_early=True,expect_exit=2)
        self.assertEqual(result['status'],'unavailable')
        self.assertNotIn('total_count',result)
        self.assertEqual(counts['open|'+str(path.resolve())],1,counts)
        self.assertEqual(counts['close|'+str(path.resolve())],1,counts)
