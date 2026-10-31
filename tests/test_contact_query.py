"""Synthetic encrypted sources exercise the public contact window entry."""
import contextlib
import hashlib
import io
import json
import os
import shutil
import unittest
from unittest.mock import patch
from tests import test_codex_first_voice_cli as fixture
import wechat_local.codex_first_runtime as runtime
import wechat_local.live_catalog as catalog
import wechat_local.image_runtime as image_runtime

TARGET='synthetic-contact';OTHER='synthetic-other';SELF='synthetic-self';GROUP2='synthetic-second@chatroom'
def table(username):return 'Msg_'+hashlib.md5(username.encode()).hexdigest()

class ContactQueryTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.CodexFirstVoiceCliTests();self.f.setUp()
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('CREATE TABLE contact(username TEXT, alias TEXT, remark TEXT, nick_name TEXT)')
            c.executemany('INSERT INTO contact VALUES(?,?,?,?)',[(TARGET,'person-alias','Target Remark','Target Nick'),(OTHER,'other-alias','Other Remark','Other Nick')])
            c.executemany('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',[(9,TARGET),(10,OTHER),(11,SELF),(12,GROUP2)])
            for username in (TARGET,GROUP2):
                c.execute('CREATE TABLE "'+table(username)+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.executemany('INSERT INTO "'+table(TARGET)+'" VALUES(?,?,?,?,?,?,?,?)',[(20,201,1,1700000000,9,b'private incoming',0,None),(21,202,1,1700000001,11,b'private outgoing',0,None),(22,203,1,1699999900,9,b'outside window',0,None)])
            c.executemany('INSERT INTO "'+table(GROUP2)+'" VALUES(?,?,?,?,?,?,?,?)',[(30,301,1,1700000002,9,b'target group',0,None),(31,302,1,1700000003,10,b'other group',0,None)])
            c.execute('INSERT INTO "'+fixture.TABLE+'" VALUES(40,401,1,1700000004,999,?,0,NULL)',(b'unknown sender',))
            c.commit()
        finally:c.close()
    def tearDown(self):self.f.tearDown()
    def run_cli(self,*extra,contact=True,contact_path=None):
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username',SELF,'--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--json']
        if contact:argv+=['--encrypted-contact-database',str(contact_path or f.message_path)]
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            try:code=runtime.main(argv+list(extra))
            except SystemExit as exc:code=exc.code
        lines=out.getvalue().splitlines();return code,json.loads(lines[-1]) if lines else {},err.getvalue()
    def test_unique_remark_resolves_and_private_preserves_both_sides(self):
        code,result,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(code,0);self.assertEqual(result['target_resolution']['selected_username'],TARGET)
        self.assertEqual(result['total_count'],2)
        refs=[x for g in result['groups'] for x in g['event_refs']]
        self.assertEqual({x['citation_text'] for x in refs},{'private incoming','private outgoing'})
    def test_related_groups_target_sender_only_before_limit_and_general_query_unchanged(self):
        code,result,_=self.run_cli('--contact-query','Target Nick','--include-related-groups','--display-limit','1')
        self.assertEqual(code,0);self.assertEqual(result['total_count'],5)
        self.assertIn('related_sender_unknown',result['gaps'])
        self.assertTrue(any(g['continuation'] for g in result['groups']))
        code,normal,_=self.run_cli()
        self.assertEqual(code,0);self.assertGreaterEqual(normal['total_count'],7)
        self.assertIn('other group',{x['citation_text'] for group in normal['groups'] for x in group['event_refs']})
        self.assertIn('unknown sender',{x['citation_text'] for group in normal['groups'] for x in group['event_refs']})
    def test_not_found_and_partial_do_not_scan_messages(self):
        for q,status in [('no-such-contact','not_found'),('Target','suggestions')]:
            with self.subTest(q=q),patch.object(runtime.CodexFirstRuntime,'scan',side_effect=AssertionError('must not scan')):
                code,result,_=self.run_cli('--contact-query',q)
            self.assertEqual(code,2);self.assertEqual(result.get('target_resolution',{}).get('status'),status)
    def test_ambiguous_exact_name_does_not_guess(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('UPDATE contact SET remark=? WHERE username=?',('Target Remark',OTHER));c.commit()
        finally:c.close()
        with patch.object(runtime.CodexFirstRuntime,'scan',side_effect=AssertionError('must not scan')):
            code,result,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(code,3);self.assertEqual(result['target_resolution']['status'],'ambiguous')
    def test_missing_contact_source_does_not_scan_all_messages(self):
        with patch.object(runtime.CodexFirstRuntime,'scan',side_effect=AssertionError('must not scan')):
            code,result,_=self.run_cli('--contact-query','Target Remark',contact=False)
        self.assertEqual(code,2);self.assertEqual(result.get('error_code'),'contact_source_not_configured')

    def test_explicit_username_wins_over_another_contacts_matching_name(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('UPDATE contact SET remark=? WHERE username=?',(TARGET,OTHER));c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query',TARGET)
        self.assertEqual(code,0);self.assertEqual(result['target_resolution']['selected_username'],TARGET)
        self.assertEqual(result['target_resolution']['match_basis'],'exact_username')

    def test_duplicate_identity_rejected_for_all_unique_contact_fields(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('INSERT INTO contact VALUES(?,?,?,?)',(TARGET,'alias2','remark2','name2'));c.commit()
        finally:c.close()
        for value in (TARGET,'Target Remark','Target Nick','person-alias','remark2','name2','alias2'):
            with self.subTest(value=value):
                code,result,_=self.run_cli('--contact-query',value)
                self.assertEqual(code,2);self.assertEqual(result['target_resolution']['status'],'unavailable')
                self.assertEqual(result['target_resolution']['reason'],'contact_identity_conflict')
                self.assertNotIn('groups',result)

    def test_suggestions_are_bounded_and_counted(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.executemany('INSERT INTO contact VALUES(?,?,?,?)',[(f'extra-{i}','','','Shared candidate') for i in range(25)]);c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query','Shared')
        self.assertEqual(code,2);res=result['target_resolution'];self.assertEqual(res['status'],'suggestions')
        self.assertEqual(res['total_candidates'],25);self.assertEqual(len(res['candidates']),20);self.assertTrue(res['truncated'])

    def test_ambiguous_remark_exposes_disambiguation_fields_without_messages(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('UPDATE contact SET remark=? WHERE username=?',('Shared Remark',OTHER));c.execute('UPDATE contact SET remark=? WHERE username=?',('Shared Remark',TARGET));c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query','Shared Remark')
        self.assertEqual(code,3);self.assertNotIn('groups',result)
        candidates={item['username']:item for item in result['target_resolution']['candidates']}
        self.assertEqual((candidates[TARGET]['alias'],candidates[TARGET]['nick_name']),('person-alias','Target Nick'))
        self.assertEqual((candidates[OTHER]['alias'],candidates[OTHER]['nick_name']),('other-alias','Other Nick'))
        code,result,_=self.run_cli('--contact-query',TARGET)
        self.assertEqual(code,0);self.assertEqual(result['target_resolution']['selected_username'],TARGET)

    def test_missing_private_table_is_not_complete_empty(self):
        code,result,_=self.run_cli('--contact-query','Other Remark')
        self.assertEqual(code,0);self.assertEqual(result['status'],'unavailable')
        self.assertIn('target_private_source_unavailable',result['gaps'])
        code,result,_=self.run_cli('--contact-query','Other Remark','--include-related-groups')
        self.assertEqual(code,0);self.assertIn('target_private_source_unavailable',result['gaps'])
        self.assertEqual(result['coverage_status'],'partial')
        refs=[x for g in result['groups'] for x in g['event_refs']]
        self.assertEqual([x['citation_text'] for x in refs],['other group'])

    def test_shared_contact_message_source_opens_and_closes_once(self):
        count={'open':0,'close':0};orig=catalog.open_encrypted_readonly
        class Wrapper:
            def __init__(self,conn):self.conn=conn
            def close(self):count['close']+=1;return self.conn.close()
            def __getattr__(self,name):return getattr(self.conn,name)
        def open_source(*args,**kwargs):
            connection,key=orig(*args,**kwargs);count['open']+=1;return Wrapper(connection),key
        with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok');self.assertEqual(count,{'open':1,'close':1})

    def test_replaced_contact_source_after_resolution_fails_closed(self):
        contact=self.f.voice_path
        c=fixture._open_encrypted_writer(contact)
        try:c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)');c.execute('INSERT INTO contact VALUES(?,?,?,?)',(TARGET,'','Target Remark',''));c.commit()
        finally:c.close()
        original=runtime.LiveIdentityAdapter.resolve_contact
        def swap(adapter,*args,**kwargs):
            result=original(adapter,*args,**kwargs)
            replacement=contact.with_name('contact-replacement.db');shutil.copyfile(contact,replacement);os.replace(replacement,contact)
            return result
        with patch.object(runtime.LiveIdentityAdapter,'resolve_contact',new=swap):
            code,result,_=self.run_cli('--contact-query','Target Remark',contact_path=contact)
        self.assertEqual(code,2);self.assertEqual(result['error_code'],'contact_source_changed')
        self.assertNotIn('groups',result)

    def test_large_confirmation_binds_contact_and_group_option(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('CREATE TABLE "'+table(OTHER)+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.executemany('INSERT INTO "'+table(OTHER)+'" VALUES(?,?,?,?,?,?,?,?)',[(50,501,1,1700000000,10,b'other one',0,None),(51,502,1,1700000001,11,b'other two',0,None)])
            c.commit()
        finally:c.close()
        window=['--request','从 2023-11-14 00:00 到 2023-11-17 00:00']
        code,first,_=self.run_cli('--contact-query','Target Remark',*window)
        self.assertEqual(code,0);self.assertEqual(first['status'],'confirmation_required')
        token=first['confirmation']['token'];self.assertEqual(first['target_resolution']['selected_username'],TARGET)
        code,second,_=self.run_cli('--contact-query','Other Remark','--confirmation-token',token,*window)
        self.assertEqual(code,0);self.assertEqual(second['status'],'confirmation_required')
        self.assertNotEqual(second['confirmation']['token'],token)
        code,third,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--confirmation-token',token,*window)
        self.assertEqual(code,0);self.assertEqual(third['status'],'confirmation_required')
        self.assertNotEqual(third['confirmation']['token'],token)

    def test_contact_change_after_stream_start_has_failed_complete(self):
        contact=self.f.voice_path
        c=fixture._open_encrypted_writer(contact)
        try:c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)');c.execute('INSERT INTO contact VALUES(?,?,?,?)',(TARGET,'','Target Remark',''));c.commit()
        finally:c.close()
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username',SELF,'--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(contact),'--contact-query','Target Remark']
        original=runtime.CodexFirstRuntime._enrich_visible_images
        def swap(instance,*args,**kwargs):
            result=original(instance,*args,**kwargs)
            replacement=contact.with_name('contact-replacement.db');shutil.copyfile(contact,replacement);os.replace(replacement,contact)
            return result
        out,err=io.StringIO(),io.StringIO()
        with patch.object(runtime.CodexFirstRuntime,'_enrich_visible_images',new=swap),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
        phases=[json.loads(x) for x in out.getvalue().splitlines()];complete=[x for x in phases if x.get('phase')=='complete']
        self.assertEqual(code,2);self.assertEqual(phases[0]['phase'],'messages')
        self.assertEqual(len(complete),1);self.assertEqual(phases[-1]['status'],'unavailable')
        self.assertEqual(phases[-1]['error_code'],'contact_source_changed')
        self.assertEqual(phases[-1]['target_resolution']['status'],'unavailable')

    def test_jsonl_success_and_suggestions_have_consistent_target_terminal(self):
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username',SELF,'--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(f.message_path)]
        def run(query):
            out,err=io.StringIO(),io.StringIO()
            with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv+['--contact-query',query])
            return code,[json.loads(x) for x in out.getvalue().splitlines()]
        code,events=run('Target Remark');self.assertEqual(code,0)
        self.assertEqual(events[0]['phase'],'messages');self.assertEqual(events[-1]['phase'],'complete')
        self.assertEqual(events[0]['result']['target_resolution'],events[-1]['target_resolution'])
        code,events=run('Target');self.assertEqual(code,2);self.assertEqual(len(events),1)
        self.assertEqual(events[0]['phase'],'complete');self.assertEqual(events[0]['target_resolution']['status'],'suggestions')

    def test_options_reject_empty_mutual_or_missing_contact_query(self):
        for args in (('--contact-query',' '),('--include-related-groups',),('--contact-query',TARGET,'--conversation-username',TARGET),('--contact-query',TARGET,'--check-config')):
            with self.subTest(args=args):
                code,_,_=self.run_cli(*args)
                self.assertEqual(code,2)

    def test_unidentifiable_group_source_is_reported_without_guessing(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('DELETE FROM Name2Id WHERE user_name=?',(GROUP2,));c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertEqual(code,0);self.assertIn('related_source_identity_unknown',result['gaps'])
        refs=[x for group in result['groups'] for x in group['event_refs']]
        self.assertFalse(any(x['citation_text']=='target group' for x in refs))

    def test_unsupported_contact_schema_returns_specific_failure(self):
        contact=self.f.voice_path
        code,result,_=self.run_cli('--contact-query','Target Remark',contact_path=contact)
        self.assertEqual(code,2);self.assertEqual(result['error_code'],'contact_source_unsupported')
        self.assertNotIn('groups',result)

    def test_related_groups_filters_other_sender_and_out_of_window(self):
        code,result,_=self.run_cli('--contact-query','person-alias','--include-related-groups')
        self.assertEqual(code,0);refs=[x for group in result['groups'] for x in group['event_refs']]
        texts={x['citation_text'] for x in refs}
        self.assertIn('target group',texts);self.assertNotIn('other group',texts)
        self.assertNotIn('unknown sender',texts);self.assertNotIn('outside window',texts)
        group=[x for group in result['groups'] if group['conversation_kind']=='群聊' for x in group['event_refs']]
        self.assertTrue(group);self.assertTrue(all(x['sender_identity']==TARGET for x in group))

    def test_filtered_group_media_never_reaches_image_or_voice_enrichment(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.executemany('INSERT INTO "'+table(GROUP2)+'" VALUES(?,?,?,?,?,?,?,?)',[(32,303,3,1700000005,10,b'other image',0,None),(33,304,34,1700000006,10,None,0,None),(34,305,3,1700000007,9,b'target image',0,None)])
            c.commit()
        finally:c.close()
        attachments=self.f.root/'attachments';attachments.mkdir();image_ids=[];voice_ids=[]
        original_image=image_runtime.enrich_bound_images
        def image(connection,identity,**kwargs):
            image_ids.append(identity.local_id)
            return original_image(connection,identity,**kwargs)
        original_voice=runtime.VoiceTranscriptEnricher.enrich
        def voice(instance,connection,binding,**kwargs):
            voice_ids.append(binding.local_id)
            return original_voice(instance,connection,binding,**kwargs)
        with patch.object(image_runtime,'enrich_bound_images',side_effect=image),patch.object(runtime.VoiceTranscriptEnricher,'enrich',new=voice):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--voice-transcripts','--voice-database',str(self.f.voice_path),'--image-database',str(self.f.voice_path),'--image-attachments-root',str(attachments),'--image-output-root',str(self.f.root/'decoded'))
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
        self.assertEqual(image_ids,[34]);self.assertTrue(voice_ids)
        self.assertNotIn(33,voice_ids);self.assertNotIn(32,image_ids)
