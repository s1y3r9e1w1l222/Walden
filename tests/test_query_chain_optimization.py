"""Focused synthetic regressions for bounded query work and connections."""
import sqlite3,unittest,contextlib,io,json
from datetime import datetime,timezone
from unittest.mock import patch
from tests.test_contact_query import ContactQueryTests,GROUP2,table
from tests import test_codex_first_voice_cli as fixture
from wechat_local.event_store import EventStore,SourceKey,RawMessage,WindowIngestRun
from wechat_local.live_catalog import LiveCatalog
from wechat_local import codex_first_runtime as runtime
from wechat_local import normalization_worker as normalizer

class ExactWindowTests(ContactQueryTests):
 def test_exact_projection_does_not_open_unused_repository(self):
  with patch.object(runtime,'StandardEventRepository',side_effect=AssertionError('unused repository opened')):
   code,result,_=self.run_cli('--contact-query','Target Remark')
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok')

 def test_regular_window_does_not_consume_unrelated_backlog(self):
  source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(GROUP2))
  msg=RawMessage(99,999,1,1700000005,10,b'old unrelated pending',0)
  window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000005,'source_max_time':1700000005,'examined_count':1})
  with EventStore(self.f.event_store_path) as store:
   cat=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation');cat.close()
   store.commit_window_messages(source,[msg],window)
   outbox=store._conn.execute('SELECT o.id FROM processing_outbox o JOIN event_revisions r ON r.id=o.revision_id WHERE r.raw_content=?',(b'old unrelated pending',)).fetchone()[0]
   state=store.ensure_outbox_consumer_state(outbox,normalizer.CONSUMER_NAME,normalizer.CONTRACT_VERSION,normalizer.NORMALIZER_VERSION)
   store._conn.execute("UPDATE outbox_consumer_state SET status='processing', lease_owner='old-worker', lease_expires_at='2001-01-01T00:00:00+00:00' WHERE id=?",(state.id,));store._conn.commit()
   before=tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone())
  code,result,_=self.run_cli()
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
  with EventStore(self.f.event_store_path) as store:
   self.assertEqual(tuple(store._conn.execute('SELECT status,lease_owner,lease_expires_at FROM outbox_consumer_state WHERE id=?',(state.id,)).fetchone()),before)
   self.assertEqual(store._conn.execute('SELECT count(*) FROM standard_event_materializations m JOIN processing_outbox o ON o.revision_id=m.source_revision_id WHERE o.id=?',(outbox,)).fetchone()[0],0)
  c=sqlite3.connect(self.f.agent_store_path)
  try:
   self.assertEqual(c.execute("SELECT count(*) FROM event_search_fts WHERE searchable_text LIKE '%old unrelated pending%'").fetchone()[0],0)
   self.assertEqual(c.execute('SELECT COALESCE(max(last_materialization_id),0) FROM projection_checkpoints').fetchone()[0],0)
  finally:c.close()

 def test_stream_reuses_contact_status_and_preserves_final_epoch_guard(self):
  f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username','synthetic-self','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(f.message_path)]
  original=runtime.LiveIdentityAdapter.contact_source_status;calls=[]
  def status(identity):calls.append(True);return original(identity)
  out,err=io.StringIO(),io.StringIO()
  with patch.object(runtime.LiveIdentityAdapter,'contact_source_status',new=status),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
  rows=[json.loads(x) for x in out.getvalue().splitlines()]
  self.assertEqual(code,0);self.assertEqual(rows[-1]['phase'],'complete');self.assertEqual(len(calls),1)

 def test_stream_contact_change_after_status_fails_terminally(self):
  f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username','synthetic-self','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(f.message_path)]
  original=runtime.CodexFirstRuntime.output_preflight
  def changed(instance,window):
   value=original(instance,window);c=fixture._open_encrypted_writer(f.message_path)
   try:c.execute('UPDATE contact SET remark=? WHERE username=?',('Changed','synthetic-contact'));c.commit()
   finally:c.close()
   return value
  out,err=io.StringIO(),io.StringIO()
  with patch.object(runtime.CodexFirstRuntime,'output_preflight',new=changed),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
  rows=[json.loads(x) for x in out.getvalue().splitlines()];terminal=[x for x in rows if x.get('phase')=='complete']
  self.assertEqual(code,2);self.assertEqual(len(terminal),1);self.assertEqual(terminal[0]['status'],'unavailable')

class EarlyNameResolutionTests(ContactQueryTests):
 def test_group_name_resolves_to_conversation_without_contact_group_scan(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:c.execute('INSERT INTO contact VALUES(?,?,?,?)',(GROUP2,'group-alias','Synthetic Group',''));c.commit()
  finally:c.close()
  code,result,_=self.run_cli('--group-query','Synthetic Group')
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
  self.assertEqual(result['target_resolution']['selected_username'],GROUP2)
  self.assertEqual({x['citation_text'] for g in result['groups'] for x in g['event_refs']},{'target group','other group'})

 def test_not_found_contact_exits_before_opening_message_catalog(self):
  with patch.object(runtime.LiveCatalog,'dynamic_encrypted',side_effect=AssertionError('message catalog opened')) as catalog:
   code,result,_=self.run_cli('--contact-query','no-such-contact')
  self.assertEqual(code,2);self.assertEqual(result.get('target_resolution',{}).get('status'),'not_found');catalog.assert_not_called()

 def test_ambiguous_group_exits_before_opening_message_catalog(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:
   c.executemany('INSERT INTO contact VALUES(?,?,?,?)',[(GROUP2,'','Common Group',''),('other-group@chatroom','','Common Group','')]);c.commit()
  finally:c.close()
  with patch.object(runtime.LiveCatalog,'dynamic_encrypted',side_effect=AssertionError('message catalog opened')) as catalog:
   code,result,_=self.run_cli('--group-query','Common Group')
  self.assertEqual(code,3);self.assertEqual(result.get('target_resolution',{}).get('status'),'ambiguous');catalog.assert_not_called()

 def test_same_group_and_person_name_uses_explicit_kind(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:
   c.execute('UPDATE contact SET remark=? WHERE username=?',('Shared Name','synthetic-contact'))
   c.execute('INSERT INTO contact VALUES(?,?,?,?)',(GROUP2,'','Shared Name',''))
   c.execute('UPDATE "'+table(GROUP2)+'" SET real_sender_id=10 WHERE local_id=30');c.commit()
  finally:c.close()
  code,group,_=self.run_cli('--group-query','Shared Name')
  self.assertEqual(code,0);self.assertEqual(group['target_resolution']['selected_username'],GROUP2)
  self.assertEqual({x['citation_text'] for g in group['groups'] for x in g['event_refs']},{'target group','other group'})
  code,person,_=self.run_cli('--contact-query','Shared Name')
  self.assertEqual(code,0);self.assertEqual(person['target_resolution']['selected_username'],'synthetic-contact')
 def test_wrong_explicit_target_kind_is_explained_without_messages(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:c.execute('INSERT INTO contact VALUES(?,?,?,?)',(GROUP2,'','Synthetic Group',''));c.commit()
  finally:c.close()
  for flag,value,hint in (('--contact-query',GROUP2,'group-query'),('--contact-query','Synthetic Group','group-query'),('--group-query','Target Remark','contact-query')):
   with self.subTest(flag=flag,value=value):
    code,result,_=self.run_cli(flag,value)
    self.assertEqual(code,2);resolution=result['target_resolution']
    self.assertEqual(resolution.get('reason'),'contact_target_type_mismatch');self.assertEqual(resolution.get('hint'),hint)
    self.assertNotIn('groups',result)
 def test_contact_voice_and_image_share_one_connection(self):
  import wechat_local.live_catalog as catalog
  contact=self.f.voice_path;c=fixture._open_encrypted_writer(contact)
  try:c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)');c.execute('INSERT INTO contact VALUES(?,?,?,?)',('synthetic-contact','','Target Remark',''));c.commit()
  finally:c.close()
  attachments=self.f.root/'shared-attachments';attachments.mkdir();count={'open':0,'close':0};original=catalog.open_encrypted_readonly
  class Wrapped:
   def __init__(self,conn):self.conn=conn
   def close(self):count['close']+=1;return self.conn.close()
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(*args,**kwargs):
   conn,key=original(*args,**kwargs);count['open']+=1;return Wrapped(conn),key
  with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source):
   code,result,_=self.run_cli('--contact-query','Target Remark','--voice-transcripts','--voice-database',str(contact),'--image-database',str(contact),'--image-attachments-root',str(attachments),'--image-output-root',str(self.f.root/'decoded-shared'),contact_path=contact)
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok');self.assertEqual(count,{'open':2,'close':2})
 def test_duplicate_group_identity_fails_before_catalog(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:c.executemany('INSERT INTO contact VALUES(?,?,?,?)',[(GROUP2,'','Group One',''),(GROUP2,'','Group Two','')]);c.commit()
  finally:c.close()
  with patch.object(runtime.LiveCatalog,'dynamic_encrypted',side_effect=AssertionError('message catalog opened')) as catalog:
   code,result,_=self.run_cli('--group-query','Group One')
  self.assertEqual(code,2);self.assertEqual(result['error_code'],'contact_identity_conflict');catalog.assert_not_called()
 def test_missing_name_reads_contact_metadata_without_message_body_or_store(self):
  import wechat_local.live_catalog as catalog
  contact=self.f.voice_path;c=fixture._open_encrypted_writer(contact)
  try:c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)');c.execute('INSERT INTO contact VALUES(?,?,?,?)',('synthetic-contact','','Target Remark',''));c.commit()
  finally:c.close()
  reads=[];opens=[];original=runtime.open_encrypted_readonly
  def open_contact(path,*args,**kwargs):
   conn,key=original(path,*args,**kwargs);opens.append(path)
   def observe(action,table_name,column,_db,_trigger):
    if action==sqlite3.SQLITE_READ:reads.append((table_name,column))
    return sqlite3.SQLITE_OK
   conn.set_authorizer(observe);return conn,key
  with patch.object(runtime,'open_encrypted_readonly',side_effect=open_contact),patch.object(catalog,'open_encrypted_readonly',side_effect=AssertionError('message source opened')),patch.object(runtime,'EventStore',side_effect=AssertionError('store opened')):
   code,result,_=self.run_cli('--contact-query','no-such-contact',contact_path=contact)
  self.assertEqual(code,2);self.assertEqual(result['target_resolution']['status'],'not_found');self.assertEqual(len(opens),1)
  self.assertTrue(any(table_name=='contact' for table_name,_ in reads));self.assertFalse(any((table_name or '').startswith('Msg_') for table_name,_ in reads))
 def test_unknown_group_identifier_in_private_entry_reports_type_hint(self):
  code,result,_=self.run_cli('--contact-query','unseen-synthetic@chatroom')
  self.assertEqual(code,2);self.assertEqual(result.get('error_code'),'contact_target_type_mismatch');self.assertEqual(result.get('target_resolution',{}).get('hint'),'group-query')
 def test_group_name_changed_after_resolution_fails_closed(self):
  c=fixture._open_encrypted_writer(self.f.message_path)
  try:c.execute('INSERT INTO contact VALUES(?,?,?,?)',(GROUP2,'','Synthetic Group',''));c.commit()
  finally:c.close()
  original=runtime.LiveIdentityAdapter.resolve_contact
  def change(adapter,*args,**kwargs):
   result=original(adapter,*args,**kwargs);c=fixture._open_encrypted_writer(self.f.message_path)
   try:c.execute('UPDATE contact SET remark=? WHERE username=?',('Renamed Group',GROUP2));c.commit()
   finally:c.close()
   return result
  with patch.object(runtime.LiveIdentityAdapter,'resolve_contact',new=change):code,result,_=self.run_cli('--group-query','Synthetic Group')
  self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable');self.assertNotIn('groups',result)

 def test_contact_message_voice_and_image_share_one_source(self):
  import wechat_local.live_catalog as catalog
  contact=self.f.message_path;attachments=self.f.root/'all-shared-attachments';attachments.mkdir();count={'open':0,'close':0};original=catalog.open_encrypted_readonly
  class Wrapped:
   def __init__(self,conn):self.conn=conn
   def close(self):count['close']+=1;return self.conn.close()
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(*args,**kwargs):
   conn,key=original(*args,**kwargs);count['open']+=1;return Wrapped(conn),key
  with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source):
   code,result,_=self.run_cli('--contact-query','Target Remark','--voice-transcripts','--voice-database',str(contact),'--image-database',str(contact),'--image-attachments-root',str(attachments),'--image-output-root',str(self.f.root/'all-shared-output'),contact_path=contact)
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok');self.assertEqual(count,{'open':1,'close':1})

 def test_preopened_contact_catalog_failure_closes_once(self):
  import wechat_local.live_catalog as catalog
  count={'open':0,'close':0};original_open=catalog.open_encrypted_readonly;original_set=catalog.LiveCatalog._set_shards
  class Wrapped:
   def __init__(self,conn):self.conn=conn
   def close(self):count['close']+=1;return self.conn.close()
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(*args,**kwargs):
   conn,key=original_open(*args,**kwargs);count['open']+=1;return Wrapped(conn),key
  def fail_catalog(instance,items):
   if items:raise ValueError('synthetic catalog error')
   return original_set(instance,items)
  with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source),patch.object(catalog.LiveCatalog,'_set_shards',new=fail_catalog):code,result,_=self.run_cli('--contact-query','Target Remark')
  self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable');self.assertEqual(count,{'open':1,'close':1})
 def test_group_query_jsonl_missing_has_single_terminal(self):
  f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username','synthetic-self','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--encrypted-contact-database',str(f.message_path),'--group-query','no matching group']
  out,err=io.StringIO(),io.StringIO()
  with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
  rows=[json.loads(x) for x in out.getvalue().splitlines()]
  self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'not_found')

 def test_related_source_filter_already_excludes_other_sender_before_facts(self):
  original=runtime.LiveIdentityAdapter.source_facts;seen=[]
  def facts(identity,source,message):seen.append((source.table_name,message.local_id));return original(identity,source,message)
  with patch.object(runtime.LiveIdentityAdapter,'source_facts',new=facts):code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups')
  self.assertEqual(code,0);self.assertEqual(result['status'],'ok')
  self.assertIn((table(GROUP2),30),seen);self.assertNotIn((table(GROUP2),31),seen)
 def test_preopened_factory_return_error_and_interrupt_close_once(self):
  import sys
  import wechat_local.live_catalog as catalog
  for mode in ('before_error','before_interrupt','error','interrupt'):
   with self.subTest(mode=mode):
    count={'open':0,'close':0,'injected':False};retained=[];original=catalog.open_encrypted_readonly
    class Wrapped:
     def __init__(self,conn):self.conn=conn
     def close(self):count['close']+=1;return self.conn.close()
     def __getattr__(self,name):return getattr(self.conn,name)
    def open_source(*args,**kwargs):
     conn,key=original(*args,**kwargs);count['open']+=1;wrapped=Wrapped(conn);retained.append(wrapped);return wrapped,key
    def probe(frame,event,arg):
     target = frame.f_code.co_filename.endswith('/live_catalog.py') and frame.f_code.co_name=='open_source'
     boundary = (mode.startswith('before_') and event=='call') or (not mode.startswith('before_') and event=='return' and isinstance(arg,Wrapped))
     if target and boundary and not count['injected']:
      count['injected']=True
      if mode.endswith('error'):raise ValueError('synthetic factory boundary')
      raise KeyboardInterrupt()
    try:
     with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source):
      sys.setprofile(probe)
      if mode.endswith('error'):code,result,_=self.run_cli('--contact-query','Target Remark');self.assertEqual(code,2)
      else:
       with self.assertRaises(KeyboardInterrupt):self.run_cli('--contact-query','Target Remark')
    finally:sys.setprofile(None)
    self.assertTrue(count['injected']);self.assertEqual((count['open'],count['close']),(1,1))
 def test_owned_close_failure_does_not_skip_remaining_or_mask_original(self):
  import wechat_local.live_catalog as catalog
  counts={'message':0,'voice':0};original_open=catalog.open_encrypted_readonly
  class Wrapped:
   def __init__(self,conn,kind):self.conn=conn;self.kind=kind
   def close(self):
    counts[self.kind]+=1;self.conn.close()
    if self.kind=='voice':raise OSError('synthetic close failure')
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(path,*args,**kwargs):
   conn,key=original_open(path,*args,**kwargs);return Wrapped(conn,'voice' if path.resolve()==self.f.voice_path.resolve() else 'message'),key
  with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime.CodexFirstRuntime,'scan',side_effect=ValueError('synthetic original')),patch.object(runtime,'_diagnostic_reason',side_effect=lambda error:type(error).__name__):
   code,result,_=self.run_cli('--contact-query','Target Remark','--voice-transcripts','--voice-database',str(self.f.voice_path))
  self.assertEqual(code,2);self.assertEqual(result['diagnostics']['failures'][0]['reason'],'ValueError');self.assertEqual(counts,{'message':1,'voice':1})
 def test_factory_interrupt_also_closes_already_opened_other_shard(self):
  import sys,shutil
  import wechat_local.live_catalog as catalog
  second=self.f.message_root/'message_1.db';shutil.copyfile(self.f.message_path,second)
  count={'open':0,'close':0,'injected':False};retained=[];original=catalog.open_encrypted_readonly
  class Wrapped:
   def __init__(self,conn,kind):self.conn=conn;self.kind=kind
   def close(self):count['close']+=1;return self.conn.close()
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(path,*args,**kwargs):
   conn,key=original(path,*args,**kwargs);count['open']+=1;wrapped=Wrapped(conn,'contact' if path.resolve()==second.resolve() else 'other');retained.append(wrapped);return wrapped,key
  def probe(frame,event,arg):
   if frame.f_code.co_filename.endswith('/live_catalog.py') and frame.f_code.co_name=='open_source' and event=='return' and isinstance(arg,Wrapped) and arg.kind=='contact' and not count['injected']:
    count['injected']=True;raise KeyboardInterrupt()
  try:
   with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source):
    sys.setprofile(probe)
    with self.assertRaises(KeyboardInterrupt):self.run_cli('--contact-query','Target Remark',contact_path=second)
  finally:sys.setprofile(None)
  self.assertTrue(count['injected']);self.assertEqual((count['open'],count['close']),(2,2))
 def test_catalog_record_failure_cleanup_preserves_original_and_closes_owned(self):
  import shutil
  import wechat_local.live_catalog as catalog
  second=self.f.message_root/'message_1.db';shutil.copyfile(self.f.message_path,second)
  counts={'open':0,'close':0,'set':0};retained=[];original=catalog.open_encrypted_readonly;original_set=catalog.LiveCatalog._set_shards
  class Wrapped:
   def __init__(self,conn):self.conn=conn
   def close(self):counts['close']+=1;return self.conn.close()
   def __getattr__(self,name):return getattr(self.conn,name)
  def open_source(*args,**kwargs):
   conn,key=original(*args,**kwargs);counts['open']+=1;wrapped=Wrapped(conn);retained.append(wrapped);return wrapped,key
  def set_shards(instance,items):
   counts['set']+=1
   if counts['set']==3:raise OSError('synthetic restore failure')
   return original_set(instance,items)
  with patch.object(catalog,'open_encrypted_readonly',side_effect=open_source),patch.object(runtime,'open_encrypted_readonly',side_effect=open_source),patch.object(catalog.LiveCatalog,'_set_shards',new=set_shards),patch.object(runtime.EventStore,'record_catalog_refresh',side_effect=ValueError('synthetic original')),patch.object(runtime,'_diagnostic_reason',side_effect=lambda error:type(error).__name__):
   code,result,_=self.run_cli('--contact-query','Target Remark',contact_path=second)
  self.assertEqual(code,2);self.assertEqual(result['diagnostics']['failures'][0]['reason'],'ValueError');self.assertEqual((counts['open'],counts['close']),(2,2))
 def test_catalog_close_failure_continues_other_owned_and_not_borrowed(self):
  import wechat_local.live_catalog as catalog
  closed=[]
  class Connection:
   def __init__(self,name,fail=False):self.name=name;self.fail=fail
   def close(self):
    closed.append(self.name)
    if self.fail:raise OSError('synthetic close failure')
  first=Connection('first');second=Connection('second',True);borrowed=Connection('borrowed')
  cat=object.__new__(catalog.LiveCatalog);cat._owns_connections=True;cat._borrowed_connection_ids={id(borrowed)}
  cat._shards=(catalog.LiveShard(self.f.message_path,first),catalog.LiveShard(self.f.message_path,second),catalog.LiveShard(self.f.message_path,borrowed));cat._routes={}
  with self.assertRaisesRegex(OSError,'synthetic close failure'):cat.close()
  self.assertEqual(closed,['second','first']);borrowed.close();self.assertEqual(closed,['second','first','borrowed'])
