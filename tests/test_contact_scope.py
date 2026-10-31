"""Bounded contact source and exact derived-work tests using synthetic SQLCipher."""
import contextlib,io,json,unittest
from datetime import datetime,timezone
from unittest.mock import patch
from tests import test_codex_first_voice_cli as fixture
from tests.test_contact_query import ContactQueryTests,TARGET,OTHER,SELF,GROUP2,table
from wechat_local.event_store import EventStore,SourceKey,RawMessage,WindowIngestRun
from wechat_local.live_catalog import LiveCatalog
import wechat_local.codex_first_runtime as runtime
import wechat_local.normalization_worker as normalizer
import wechat_local.event_store as event_store_module

class ScopeTests(ContactQueryTests):
    def test_explicit_context_jsonl_configuration_failure_has_one_complete(self):
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.event_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username',SELF,'--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--conversation-username',GROUP2]
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,2);self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'unavailable')
        self.assertIn('diagnostics',rows[0])

    def test_explicit_context_keeps_full_group_and_does_not_drain_old_backlog(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(TARGET))
        msg=RawMessage(99,999,1,1700000005,10,b'old unrelated pending',0)
        window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000005,'source_max_time':1700000005,'examined_count':1})
        with EventStore(self.f.event_store_path) as store:
            cat=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation');cat.close()
            store.commit_window_messages(source,[msg],window)
            outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'old unrelated pending',)).fetchone()[0]
            state=store.ensure_outbox_consumer_state(outbox,normalizer.CONSUMER_NAME,normalizer.CONTRACT_VERSION,normalizer.NORMALIZER_VERSION)
            store._conn.execute("UPDATE outbox_consumer_state SET status='processing', lease_owner='other-worker', lease_expires_at='2001-01-01T00:00:00+00:00' WHERE id=?",(state.id,));store._conn.commit()
            before=tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone())
        code,result,_=self.run_cli('--conversation-username',GROUP2)
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
        texts=[r['citation_text'] for g in result['groups'] for r in g['event_refs']]
        self.assertEqual(set(texts),{'target group','other group'})
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone()),before)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0],0)
        import sqlite3
        c=sqlite3.connect(self.f.agent_store_path)
        try:
            self.assertEqual(c.execute('SELECT COALESCE(max(last_materialization_id),0) FROM projection_checkpoints').fetchone()[0],0)
            self.assertEqual(c.execute("SELECT count(*) FROM event_search_fts WHERE searchable_text LIKE '%old unrelated pending%'").fetchone()[0],0)
        finally:c.close()

    def test_explicit_context_source_identity_snapshot_and_wal_change(self):
        from wechat_local.live_identity import _username_for_table
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('PRAGMA journal_mode=WAL');c.commit()
        finally:c.close()
        original=runtime.read_message_window;observed=[]
        def read(conn,name,*args,**kwargs):
            if name==table(GROUP2) and not observed:
                w=fixture._open_encrypted_writer(self.f.message_path)
                try:w.execute('UPDATE Name2Id SET user_name=? WHERE rowid=12',(OTHER,));w.commit()
                finally:w.close()
                observed.append(_username_for_table(conn,name))
            return original(conn,name,*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=read):code,result,_=self.run_cli('--conversation-username',GROUP2)
        self.assertEqual(observed,[GROUP2]);self.assertEqual(result['status'],'unavailable')

    def test_explicit_context_large_confirmation_is_bound_to_conversation(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('INSERT INTO "'+table(GROUP2)+'" VALUES(32,3022,1,1700000004,10,?,0,NULL)',(b'other group extra',));c.commit()
        finally:c.close()
        window=['--request','从 2023-11-14 00:00 到 2023-11-17 00:00']
        code,first,_=self.run_cli('--conversation-username',GROUP2,*window)
        self.assertEqual(first['status'],'confirmation_required');token=first['confirmation']['token']
        code,second,_=self.run_cli('--conversation-username',fixture.GROUP,'--confirmation-token',token,*window)
        self.assertEqual(second['status'],'confirmation_required');self.assertNotEqual(second['confirmation']['token'],token)

    def test_fractional_related_group_and_preflight_use_same_bounds(self):
        seen=[];original=runtime.read_message_window
        def read(connection,name,*args,**kwargs):
            result=original(connection,name,*args,**kwargs);seen.extend(m.create_time for m in result.messages);return result
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--request','从 2023-11-14 22:13:20.500000 到 2023-11-14 22:13:22.500000')
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok');self.assertEqual(result['total_count'],2)
        self.assertEqual(sorted(seen),[1700000001,1700000002])
        code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--request','从 2023-11-14 22:13:20.500000 到 2023-11-17 00:00')
        self.assertEqual(result['status'],'confirmation_required');self.assertEqual(result['confirmation']['estimated_rows'],3)

    def test_fractional_start_never_fetches_or_persists_earlier_second(self):
        seen=[]; original=runtime.read_message_window
        def read(connection,name,*args,**kwargs):
            result=original(connection,name,*args,**kwargs);seen.extend(message.raw_content for message in result.messages);return result
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--request','从 2023-11-14 22:13:20.500000 到 2023-11-14 22:13:21.500000')
        self.assertEqual(code,0);self.assertEqual(result['total_count'],1)
        self.assertNotIn(b'private incoming',seen)
        with EventStore(self.f.event_store_path) as store:self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions WHERE raw_content=?',(b'private incoming',)).fetchone()[0],0)

    def test_fractional_window_without_integer_second_has_no_body_or_proof(self):
        calls=[]; original=runtime.read_message_window
        def read(*args,**kwargs):calls.append(True);return original(*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--request','从 2023-11-14 22:13:20.100000 到 2023-11-14 22:13:20.900000')
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok');self.assertEqual(result['total_count'],0);self.assertFalse(calls)
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0],0)

    def test_group_discovery_epoch_cannot_be_overwritten_by_later_private_source(self):
        import wechat_local.live_catalog as live_catalog
        group=next('scope-earlier-'+str(i)+'@chatroom' for i in range(100) if table('scope-earlier-'+str(i)+'@chatroom')<table(TARGET))
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',(20,group))
            c.execute('CREATE TABLE "'+table(group)+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.commit()
        finally:c.close()
        contact=fixture._open_encrypted_writer(self.f.voice_path)
        try:
            contact.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)')
            contact.execute('INSERT INTO contact VALUES(?,?,?,?)',(TARGET,'person-alias','Target Remark','Target Nick'))
            contact.commit()
        finally:contact.close()
        mutation=[];reads=[];original_read=runtime.read_message_window;original_open=live_catalog.open_encrypted_readonly
        class Cursor:
            def __init__(self,cur,query):self.cur,self.query=cur,query
            def __getattr__(self,n):return getattr(self.cur,n)
            def __iter__(self):return self
            def __next__(self):return next(self.cur)
            def fetchone(self):
                row=self.cur.fetchone()
                if table(group) in self.query and "typeof(real_sender_id)='integer' AND real_sender_id=? LIMIT 1" in self.query and not mutation:
                    w=fixture._open_encrypted_writer(self_path)
                    try:w.execute('INSERT INTO "'+table(group)+'" VALUES(71,711,1,1700000001,9,?,0,NULL)',(b'new target',));w.commit();mutation.append(True)
                    finally:w.close()
                return row
        class Connection:
            def __init__(self,c):self.c=c
            def __getattr__(self,n):return getattr(self.c,n)
            def execute(self,query,*args):return Cursor(self.c.execute(query,*args),query)
        self_path=self.f.message_path
        def open_source(*args,**kwargs):
            connection,key=original_open(*args,**kwargs);return Connection(connection),key
        def read(connection,name,*args,**kwargs):
            reads.append(name);return original_read(connection,name,*args,**kwargs)
        with patch.object(live_catalog,'open_encrypted_readonly',new=open_source),patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups',contact_path=self.f.voice_path)
        self.assertTrue(mutation);self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable')
        self.assertEqual(result.get('error_code'),'source_changed')
        self.assertFalse(result.get('groups'));self.assertFalse(reads)
        with EventStore(self.f.event_store_path) as store:self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions').fetchone()[0],0)

    def test_missing_or_duplicate_sender_mapping_never_falls_back_to_group_body(self):
        for mode in ('missing','duplicate'):
            with self.subTest(mode=mode):
                c=fixture._open_encrypted_writer(self.f.message_path)
                try:
                    if mode=='missing':c.execute('DELETE FROM Name2Id WHERE user_name=?',(TARGET,))
                    else:c.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',(77,TARGET))
                    c.commit()
                finally:c.close()
                fetched=[];original=runtime.read_message_window
                def read(conn,name,*args,**kwargs):
                    if name in (table(GROUP2),fixture.TABLE):fetched.append(name)
                    return original(conn,name,*args,**kwargs)
                with patch.object(runtime,'read_message_window',new=read):
                    code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
                self.assertFalse(fetched);self.assertNotEqual(result.get('status'),'ok')
                self.assertIn('related_sender_unknown',result.get('gaps',[]))
                c=fixture._open_encrypted_writer(self.f.message_path)
                try:
                    c.execute('DELETE FROM Name2Id WHERE user_name=?',(TARGET,))
                    c.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',(9,TARGET));c.commit()
                finally:c.close()

    def test_large_window_preflight_counts_only_scoped_rows(self):
        code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--request','从 2023-11-14 00:00 到 2023-11-17 00:00')
        self.assertEqual(code,0);self.assertEqual(result['status'],'confirmation_required')
        self.assertEqual(result['confirmation']['estimated_rows'],6)

    def test_private_source_mapping_remains_pinned_before_body_fetch(self):
        from wechat_local.live_identity import _username_for_table
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('PRAGMA journal_mode=WAL');c.commit()
        finally:c.close()
        original=runtime.read_message_window;observed=[]
        def read(conn,name,*args,**kwargs):
            if name==table(TARGET) and not observed:
                w=fixture._open_encrypted_writer(self.f.message_path)
                try:w.execute('UPDATE Name2Id SET user_name=? WHERE rowid=9',(OTHER,));w.commit()
                finally:w.close()
                observed.append(_username_for_table(conn,name))
            return original(conn,name,*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(observed,[TARGET])
        self.assertEqual(result['status'],'unavailable')

    def test_name2id_change_after_snapshot_cannot_change_fetched_sender(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('PRAGMA journal_mode=WAL');c.commit()
        finally:c.close()
        original=runtime.read_message_window;seen=[];changed=[]
        class Cursor:
            def __init__(self,c,sql):self.c,self.sql=c,sql
            def fetchall(self):
                rows=self.c.fetchall()
                if 'message_content' in self.sql:seen.extend(row[5] for row in rows)
                return rows
            def __getattr__(self,n):return getattr(self.c,n)
        class Connection:
            def __init__(self,c):self.c=c
            def execute(self,sql,*a):return Cursor(self.c.execute(sql,*a),sql)
            def __getattr__(self,n):return getattr(self.c,n)
        def read(conn,name,*args,**kwargs):
            if name==table(GROUP2) and not changed:
                w=fixture._open_encrypted_writer(self.f.message_path)
                try:
                    w.execute('UPDATE Name2Id SET user_name=? WHERE rowid=9',(OTHER,))
                    w.execute('UPDATE Name2Id SET user_name=? WHERE rowid=10',(TARGET,))
                    w.commit();changed.append(True)
                finally:w.close()
            return original(Connection(conn),name,*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertTrue(changed);self.assertNotIn(b'other group',seen)
        self.assertEqual(result['status'],'unavailable')

    def test_empty_scope_does_not_create_baseline_proof_or_checkpoint(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('INSERT INTO contact VALUES(?,?,?,?)',('scope-empty','','Empty Scope',''))
            c.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',(19,'scope-empty'));c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query','Empty Scope','--include-related-groups')
        self.assertEqual(result['status'],'unavailable')
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0],0)
        import sqlite3
        c=sqlite3.connect(self.f.agent_store_path)
        try:self.assertEqual(c.execute('SELECT count(*) FROM projection_checkpoints').fetchone()[0],0)
        finally:c.close()

    def test_selected_expired_lease_is_not_stolen_or_rendered(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(TARGET))
        msg=RawMessage(20,201,1,1700000000,9,b'private incoming',0)
        window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000000,'source_max_time':1700000000,'examined_count':1})
        with EventStore(self.f.event_store_path) as store:
            catalog=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation');catalog.close()
            store.commit_window_messages(source,[msg],window)
            outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'private incoming',)).fetchone()[0]
            state=store.ensure_outbox_consumer_state(outbox,normalizer.CONSUMER_NAME,normalizer.CONTRACT_VERSION,normalizer.NORMALIZER_VERSION)
            store._conn.execute("UPDATE outbox_consumer_state SET status='processing', lease_owner='other-worker', lease_expires_at='2001-01-01T00:00:00+00:00' WHERE id=?",(state.id,));store._conn.commit()
        code,result,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable');self.assertFalse(result.get('groups'))
        with EventStore(self.f.event_store_path) as store:
            row=store._conn.execute('SELECT status,lease_owner FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone()
            self.assertEqual(tuple(row),('processing','other-worker'))

    def test_changed_current_revision_never_returns_wrong_canonical(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(GROUP2))
        old=RawMessage(99,999,1,1700000005,9,b'expected target',0)
        changed=RawMessage(99,999,1,1700000005,10,b'changed sender body',0)
        window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000005,'source_max_time':1700000005,'examined_count':1})
        with EventStore(self.f.event_store_path) as store:
            catalog=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation');catalog.close()
            store.commit_window_messages(source,[old],window)
            store.commit_window_messages(source,[changed],window)
            outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'changed sender body',)).fetchone()[0]
            self.assertEqual(normalizer.NormalizationWorker(store,'fixture-worker').process_exact(outbox).outcome,'normalized')
            from wechat_local.standard_event_repository import ProjectionProfile
            profile=ProjectionProfile()
            with patch.object(event_store_module,'_materialization_from_row',side_effect=AssertionError('unexpected canonical payload fetched')):
                self.assertEqual(store.standard_event_materializations_for_messages(source,[old],contract_version=profile.contract_version,normalizer_name=profile.normalizer_name,normalizer_version=profile.normalizer_version,_exact=True),{})
            with self.assertRaises(Exception):store._exact_window_outboxes(source,[old])

    def test_unselected_voice_and_source_columns_never_fetched(self):
        name=table(GROUP2)
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('ALTER TABLE "'+name+'" ADD COLUMN source BLOB')
            c.execute('ALTER TABLE "'+name+'" ADD COLUMN WCDB_CT_source INTEGER')
            c.execute('INSERT INTO "'+name+'" (local_id,server_id,local_type,create_time,real_sender_id,message_content,WCDB_CT_message_content,packed_info_data,source,WCDB_CT_source) VALUES (32,332,34,1700000005,10,?,0,?,?,0)',(b'other voice body',b'other voice packed',b'other source'))
            c.commit()
        finally:c.close()
        original=runtime.read_message_window;observed=[]
        class Cursor:
            def __init__(self,c,sql):self.c,self.sql=c,sql
            def fetchall(self):
                rows=self.c.fetchall()
                if 'message_content' in self.sql:observed.extend((row[5],row[7],row[9]) for row in rows)
                return rows
            def __getattr__(self,n):return getattr(self.c,n)
        class Connection:
            def __init__(self,c):self.c=c
            def execute(self,sql,*a):return Cursor(self.c.execute(sql,*a),sql)
            def __getattr__(self,n):return getattr(self.c,n)
        def read(conn,*args,**kwargs):return original(Connection(conn),*args,**kwargs)
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username','synthetic-self','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(f.message_path),'--contact-query','Target Remark','--include-related-groups']
        out,err=io.StringIO(),io.StringIO()
        with patch.object(runtime,'read_message_window',new=read),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
        self.assertEqual(code,0)
        self.assertFalse(any(v in (b'other voice body',b'other voice packed',b'other source') for row in observed for v in row))

    def test_sender_change_between_discovery_and_body_never_fetches_changed_row(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('PRAGMA journal_mode=WAL');c.commit()
        finally:c.close()
        original=runtime.read_message_window; seen=[]; changed=[]
        class Cursor:
            def __init__(self,c,sql):self.c,self.sql=c,sql
            def fetchall(self):
                rows=self.c.fetchall()
                if 'message_content' in self.sql:seen.extend(row[5] for row in rows)
                return rows
            def __getattr__(self,n):return getattr(self.c,n)
        class Connection:
            def __init__(self,c):self.c=c
            def execute(self,sql,*a):return Cursor(self.c.execute(sql,*a),sql)
            def __getattr__(self,n):return getattr(self.c,n)
        def read(conn,name,*args,**kwargs):
            if name==table(GROUP2) and not changed:
                w=fixture._open_encrypted_writer(self.f.message_path)
                try:w.execute('UPDATE "'+name+'" SET real_sender_id=9 WHERE local_id=31');w.commit();changed.append(True)
                finally:w.close()
            return original(Connection(conn),name,*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=read):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertTrue(changed);self.assertNotIn(b'other group',seen)
        self.assertEqual(result['status'],'unavailable')

    def test_distinct_shards_have_independent_sender_rowids(self):
        path=self.f.message_root/'message_1.db'
        group='scope-second-shard@chatroom';name=table(group)
        c=fixture._open_encrypted_writer(path)
        try:
            c.execute('CREATE TABLE Name2Id(user_name TEXT)')
            c.executemany('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',[(30,TARGET),(31,OTHER),(32,group)])
            c.execute('CREATE TABLE "'+name+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.executemany('INSERT INTO "'+name+'" VALUES(?,?,?,?,?,?,?,?)',[(70,701,1,1700000000,30,b'second shard target',0,None),(71,702,1,1700000001,31,b'second shard other',0,None)]);c.commit()
        finally:c.close()
        code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertEqual(code,0)
        texts=[r['citation_text'] for g in result['groups'] for r in g['event_refs']]
        self.assertIn('second shard target',texts);self.assertNotIn('second shard other',texts)
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions WHERE raw_content=?',(b'second shard other',)).fetchone()[0],0)

    def test_scope_does_not_fetch_other_sender_body_and_later_full_query_keeps_it(self):
        observed=[];queries=[]; original=runtime.read_message_window
        class Cursor:
            def __init__(self,c,sql):self.c,self.sql=c,sql
            def fetchall(self):
                rows=self.c.fetchall()
                if 'message_content' in self.sql:
                    queries.append(self.sql)
                    observed.extend(row[5] for row in rows)
                return rows
            def __getattr__(self,name):return getattr(self.c,name)
        class Connection:
            def __init__(self,c):self.c=c
            def execute(self,sql,*args):return Cursor(self.c.execute(sql,*args),sql)
            def __getattr__(self,name):return getattr(self.c,name)
        def observe(conn,*args,**kwargs):return original(Connection(conn),*args,**kwargs)
        with patch.object(runtime,'read_message_window',new=observe):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
        self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
        self.assertNotIn(b'other group',observed)
        self.assertTrue(any('real_sender_id=?' in q for q in queries))
        refs=[r for group in result['groups'] for r in group['event_refs']]
        groups=[r for r in refs if r.get('conversation_username')==GROUP2]
        self.assertTrue(groups)
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions WHERE raw_content=?',(b'other group',)).fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0],0)
        code,result,_=self.run_cli()
        self.assertEqual(code,0)
        refs=[r for group in result['groups'] for r in group['event_refs']]
        self.assertTrue(any(r.get('citation_text')=='other group' for r in refs))

    def test_private_query_does_not_process_unrelated_pending_and_checkpoint(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(GROUP2))
        with EventStore(self.f.event_store_path) as store:
            cat=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation')
            cat.close()
            msg=RawMessage(99,999,1,1700000005,10,b'old unrelated pending',0)
            old_projection=RawMessage(98,998,1,1700000004,10,b'old unrelated unprojected',0)
            window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000005,'source_max_time':1700000005,'examined_count':1})
            store.commit_window_messages(source,[msg,old_projection],window)
            outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'old unrelated pending',)).fetchone()[0]
            other_outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'old unrelated unprojected',)).fetchone()[0]
            self.assertEqual(normalizer.NormalizationWorker(store,'fixture-worker').process_exact(other_outbox).outcome,'normalized')
            state=store.ensure_outbox_consumer_state(outbox,normalizer.CONSUMER_NAME,normalizer.CONTRACT_VERSION,normalizer.NORMALIZER_VERSION)
            store._conn.execute("UPDATE outbox_consumer_state SET status='processing', lease_owner='old-worker', lease_expires_at='2001-01-01T00:00:00+00:00' WHERE id=?",(state.id,));store._conn.commit()
            before=tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone())
        seen=[]; fetched_canonical=[]; orig=normalizer.normalize_event
        def track(event,revision,*args,**kwargs):seen.append(revision.raw_content);return orig(event,revision,*args,**kwargs)
        original_materialization=event_store_module._materialization_from_row
        def track_canonical(row):
            fetched_canonical.append(str(row['canonical_payload_json']))
            return original_materialization(row)
        with patch.object(normalizer,'normalize_event',new=track),patch.object(event_store_module,'_materialization_from_row',new=track_canonical):
            code,result,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(code,0);self.assertNotIn(b'old unrelated pending',seen)
        self.assertFalse(any('old unrelated unprojected' in payload for payload in fetched_canonical))
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone()),before)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM standard_event_materializations m JOIN processing_outbox o ON o.revision_id=m.source_revision_id WHERE o.id=?',(outbox,)).fetchone()[0],0)
        import sqlite3
        c=sqlite3.connect(self.f.agent_store_path)
        try:
            self.assertEqual(c.execute('SELECT count(*) FROM event_search_projection WHERE citation_text LIKE ?',('%old unrelated pending%',)).fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM event_search_projection WHERE citation_text LIKE ?',('%old unrelated unprojected%',)).fetchone()[0],0)
            self.assertEqual(c.execute('SELECT COALESCE(max(last_materialization_id),0) FROM projection_checkpoints').fetchone()[0],0)
        finally:c.close()
