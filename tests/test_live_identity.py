import hashlib
import sqlite3
import shutil
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace

from wechat_local.event_store import RawMessage, SourceKey
from wechat_local.live_identity import LiveIdentityAdapter


def _fixture(username: str):
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE Name2Id(user_name TEXT NOT NULL)")
    connection.execute("INSERT INTO Name2Id(rowid, user_name) VALUES(7, ?)", (username,))
    connection.execute("INSERT INTO Name2Id(rowid, user_name) VALUES(8, 'sample_self_user')")
    connection.execute("PRAGMA query_only=ON")
    table_name = "Msg_" + hashlib.md5(username.encode()).hexdigest()
    source = SourceKey("synthetic-account", "synthetic-generation", "message_0.db", table_name)
    shard = SimpleNamespace(path=Path("message_0.db"), connection=connection, shard_name="message_0.db")

    class Catalog:
        shards = (shard,)

        def shard_for_source(self, candidate):
            return shard if candidate == source else None

    return connection, Catalog(), source


class LiveIdentityTests(unittest.TestCase):
    def test_explicit_contact_source_resolves_only_exact_username_and_keeps_ids_distinct(self):
        connection, catalog, source = _fixture("ordinary_username")
        contacts = sqlite3.connect(":memory:")
        contacts.execute(
            "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT)"
        )
        contacts.executemany(
            "INSERT INTO contact VALUES (?, ?, ?, ?)",
            [
                ("ordinary_username", "alias-a", "Same display", "Nick A"),
                ("other_username", "alias-b", "Same display", "Nick B"),
                ("sample_self_user", "sender-alias", "Sender Name", "Sender Nick"),
            ],
        )
        contacts.execute("PRAGMA query_only=ON")
        other_connection, other_catalog, other_source = _fixture("other_username")
        try:
            adapter = LiveIdentityAdapter(catalog, contact_connection=contacts)
            adapter.conversation_username(source)
            conversation_id = next(iter(adapter._conversation_names))
            self.assertEqual(adapter.display_name(conversation_id), "Same display")
            self.assertEqual(adapter.conversation_kind(conversation_id), "私聊或其他（未区分）")
            self.assertEqual(
                adapter.sender_identity(source, RawMessage(1, 2, 1, 1_700_000_000, 8, "synthetic", None)),
                "sample_self_user",
            )
            self.assertEqual(
                adapter.sender_display_name(source, RawMessage(1, 2, 1, 1_700_000_000, 8, "synthetic", None)),
                "Sender Name",
            )
            other_adapter = LiveIdentityAdapter(other_catalog, contact_connection=contacts)
            other_adapter.conversation_username(other_source)
            other_conversation_id = next(iter(other_adapter._conversation_names))
            self.assertNotEqual(conversation_id, other_conversation_id)
            self.assertEqual(other_adapter.display_name(other_conversation_id), "Same display")
            sql: list[str] = []
            contacts.set_trace_callback(sql.append)
            adapter.display_name(conversation_id)
            reads = [statement for statement in sql if statement.lstrip().lower().startswith("select")]
            self.assertEqual(len(reads), 1)
            self.assertIn("where username=", reads[0].lower())
        finally:
            contacts.close()
            connection.close()
            other_connection.close()

    def test_scan_does_not_repeat_connection_path_and_nested_contact_checks(self):
        connection, catalog, source = _fixture('ordinary_username')
        contacts = sqlite3.connect(':memory:')
        contacts.execute('CREATE TABLE contact(username,alias,remark,nick_name,verify_flag)')
        contacts.execute('INSERT INTO contact VALUES(?,?,?,?,?)', ('ordinary_username', '', 'name', '', 1))
        contacts.execute('PRAGMA query_only=ON')
        adapter = LiveIdentityAdapter(catalog, self_username='sample_self_user', contact_connection=contacts)
        adapter.begin_scan()
        message = RawMessage(1, 101, 1, 1700000000, 7, 'synthetic', None)
        source_sql = []; contact_sql = []
        connection.set_trace_callback(source_sql.append);contacts.set_trace_callback(contact_sql.append)
        try:
            for _ in range(3):
                adapter.direction(source, message)
                adapter.sender_display_name(source, message)
                adapter.source_facts(source, message)
            count = lambda seq, token: sum(item.strip().lower().startswith(token) for item in seq)
            self.assertEqual(count(source_sql, 'pragma database_list'), 1)
            self.assertEqual(count(contact_sql, 'pragma database_list'), 1)
            self.assertLessEqual(count(contact_sql, 'pragma data_version'), 8)
            self.assertLessEqual(count(source_sql, 'pragma table_info(name2id)'), 2)
        finally:
            contacts.close();connection.close()

    def test_one_scan_combines_contact_facts_and_detects_external_update(self):
        message_connection, catalog, source = _fixture("ordinary_username")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'contact.db'
            with sqlite3.connect(path) as writer:
                writer.execute('CREATE TABLE contact(username,alias,remark,nick_name,verify_flag)')
                writer.execute('INSERT INTO contact VALUES(?,?,?,?,?)', ('ordinary_username', '', 'before', '', 1))
            contact = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
            contact.execute('PRAGMA query_only=ON')
            try:
                adapter = LiveIdentityAdapter(catalog, contact_connection=contact)
                adapter.begin_scan()
                username = adapter.conversation_username(source)
                msg = RawMessage(1, 2, 1, 1700000000, 7, 'synthetic', None)
                queries = [];contact.set_trace_callback(queries.append)
                self.assertEqual(adapter._contact_display_name(username), 'before')
                self.assertEqual(adapter.source_facts(source, msg)['contact']['verify_flag_raw'], 1)
                self.assertEqual(adapter._contact_display_name(username), 'before')
                selects = lambda: [q for q in queries if ' from "contact" where username=' in q.lower()]
                self.assertEqual(len(selects()), 1)
                with sqlite3.connect(path) as writer:
                    writer.execute('UPDATE contact SET remark=?, verify_flag=?', ('after', 2))
                self.assertEqual(adapter._contact_display_name(username), 'after')
                self.assertEqual(adapter.source_facts(source, msg)['contact']['verify_flag_raw'], 2)
                self.assertEqual(len(selects()), 2)
                adapter.begin_scan()
                self.assertEqual(adapter._contact_display_name(username), 'after')
                self.assertEqual(len(selects()), 3)
            finally:
                contact.close()
                message_connection.close()

    def test_many_conversations_use_one_name2id_scan_per_shard(self):
        connection=sqlite3.connect(':memory:')
        connection.execute('CREATE TABLE Name2Id(user_name TEXT)')
        usernames=['synthetic_user_%03d'%i for i in range(110)]
        connection.executemany('INSERT INTO Name2Id VALUES (?)',[(x,) for x in usernames])
        connection.execute('PRAGMA query_only=ON')
        sources=[SourceKey('synthetic-account','synthetic-generation','message_0.db',
                  'Msg_'+hashlib.md5(name.encode()).hexdigest()) for name in usernames[:10]]
        shard=SimpleNamespace(path=Path('message_0.db'),connection=connection,shard_name='message_0.db')
        routes={source.table_name:(shard,) for source in sources}
        catalog=SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda source:shard if source.table_name in routes else None)
        adapter=LiveIdentityAdapter(catalog);adapter.begin_scan()
        statements=[];connection.set_trace_callback(statements.append)
        original=hashlib.md5;calls=[]
        def counted(*args,**kwargs):calls.append(1);return original(*args,**kwargs)
        try:
            with patch('wechat_local.live_identity.hashlib.md5',side_effect=counted):
                values=[adapter.conversation_username(source) for source in sources]
            self.assertEqual(values,usernames[:10])
            self.assertEqual(len(calls),len(usernames))
            self.assertEqual(sum('from name2id' in sql.lower() for sql in statements),1)
        finally:connection.close()

    def test_batched_names_preserve_duplicate_missing_and_nontext_unknown(self):
        connection=sqlite3.connect(':memory:')
        connection.execute('CREATE TABLE Name2Id(user_name)')
        connection.executemany('INSERT INTO Name2Id VALUES (?)', [('unique',),('duplicate',),('duplicate',),(None,),(b'bytes',)])
        connection.execute('PRAGMA query_only=ON')
        sources=[SourceKey('synthetic-account','synthetic-generation','message_0.db',
                 'Msg_'+hashlib.md5(name.encode()).hexdigest()) for name in ('unique','duplicate','missing')]
        shard=SimpleNamespace(path=Path('message_0.db'),connection=connection,shard_name='message_0.db')
        routes={source.table_name:(shard,) for source in sources}
        catalog=SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda source:shard if source.table_name in routes else None)
        adapter=LiveIdentityAdapter(catalog);adapter.begin_scan()
        try:self.assertEqual([adapter.conversation_username(s) for s in sources],['unique',None,None])
        finally:connection.close()

    def test_batched_names_invalidate_on_wal_changes_replacement_and_next_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'message_0.db'
            writer=sqlite3.connect(path);writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE Name2Id(user_name TEXT)')
            writer.executemany('INSERT INTO Name2Id VALUES(?)',[('user_a',),('user_b',)])
            writer.commit()
            connection=sqlite3.connect(f'file:{path}?mode=ro',uri=True);connection.execute('PRAGMA query_only=ON')
            shard=SimpleNamespace(path=path,connection=connection,shard_name=path.name)
            sources=[SourceKey('synthetic-account','synthetic-generation',path.name,'Msg_'+hashlib.md5(x.encode()).hexdigest()) for x in ('user_a','user_b')]
            routes={source.table_name:(shard,) for source in sources}
            catalog=SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda source:shard if source.table_name in routes else None)
            adapter=LiveIdentityAdapter(catalog);adapter.begin_scan()
            try:
                self.assertEqual([adapter.conversation_username(x) for x in sources],['user_a','user_b'])
                writer.execute('INSERT INTO Name2Id VALUES(?)',('user_a',));writer.commit()
                self.assertEqual([adapter.conversation_username(x) for x in sources],[None,'user_b'])
                writer.execute('DELETE FROM Name2Id WHERE user_name=?',('user_a',));writer.commit()
                self.assertEqual(adapter.conversation_username(sources[0]),None)
                writer.execute('INSERT INTO Name2Id VALUES(?)',('user_a',));writer.commit()
                adapter.begin_scan()
                self.assertEqual(adapter.conversation_username(sources[0]),'user_a')
                replacement=path.with_name('replacement.db');replacement.write_bytes(path.read_bytes());replacement.replace(path)
                self.assertIsNone(adapter.conversation_username(sources[0]))
            finally:connection.close();writer.close()

    def test_batched_names_discard_results_if_wal_changes_during_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'message_0.db';writer=sqlite3.connect(path)
            writer.execute('PRAGMA journal_mode=WAL');writer.execute('CREATE TABLE Name2Id(user_name)')
            writer.executemany('INSERT INTO Name2Id VALUES(?)',[('user_a',),('user_b',)]);writer.commit()
            raw=sqlite3.connect(f'file:{path}?mode=ro',uri=True);raw.execute('PRAGMA query_only=ON')
            class Changing:
                changed=False
                def execute(self,sql,*args):
                    rows=raw.execute(sql,*args)
                    if sql=='SELECT user_name FROM Name2Id' and not self.changed:
                        self.changed=True
                        def yielding():
                            for index,row in enumerate(rows):
                                yield row
                                if index==0:writer.execute('INSERT INTO Name2Id VALUES(?)',('user_a',));writer.commit()
                        return yielding()
                    return rows
                def __getattr__(self,name):return getattr(raw,name)
            connection=Changing();shard=SimpleNamespace(path=path,connection=connection,shard_name=path.name)
            sources=[SourceKey('synthetic-account','synthetic-generation',path.name,'Msg_'+hashlib.md5(x.encode()).hexdigest()) for x in ('user_a','user_b')]
            routes={source.table_name:(shard,) for source in sources}
            adapter=LiveIdentityAdapter(SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda _:shard));adapter.begin_scan()
            try:
                self.assertIsNone(adapter.conversation_username(sources[0]))
                self.assertEqual(adapter.conversation_username(sources[1]),'user_b')
                self.assertIsNone(adapter.conversation_username(sources[0]))
            finally:raw.close();writer.close()

    def test_batched_name_scan_discards_partial_values_on_read_error(self):
        raw=sqlite3.connect(':memory:');raw.execute('CREATE TABLE Name2Id(user_name)')
        raw.executemany('INSERT INTO Name2Id VALUES(?)',[('user_a',),('user_b',)])
        raw.execute('PRAGMA query_only=ON')
        class Broken:
            def execute(self,sql,*args):
                if sql=='SELECT user_name FROM Name2Id':
                    def rows():
                        yield ('user_a',)
                        raise sqlite3.OperationalError('synthetic')
                    return rows()
                return raw.execute(sql,*args)
            def __getattr__(self,name):return getattr(raw,name)
        connection=Broken();shard=SimpleNamespace(path=Path('message_0.db'),connection=connection,shard_name='message_0.db')
        sources=[SourceKey('synthetic-account','synthetic-generation','message_0.db','Msg_'+hashlib.md5(x.encode()).hexdigest()) for x in ('user_a','user_b')]
        routes={source.table_name:(shard,) for source in sources}
        adapter=LiveIdentityAdapter(SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda _:shard));adapter.begin_scan()
        try:self.assertEqual([adapter.conversation_username(x) for x in sources],[None,None])
        finally:raw.close()

    def test_batched_explicit_conversation_keeps_only_selected_table(self):
        connection=sqlite3.connect(':memory:');connection.execute('CREATE TABLE Name2Id(user_name)')
        connection.executemany('INSERT INTO Name2Id VALUES(?)',[('user_a',),('user_b',)])
        connection.execute('PRAGMA query_only=ON')
        shard=SimpleNamespace(path=Path('message_0.db'),connection=connection,shard_name='message_0.db')
        sources=[SourceKey('synthetic-account','synthetic-generation','message_0.db','Msg_'+hashlib.md5(x.encode()).hexdigest()) for x in ('user_a','user_b')]
        routes={source.table_name:(shard,) for source in sources}
        adapter=LiveIdentityAdapter(SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda _:shard),conversation_username='user_a');adapter.begin_scan()
        try:
            self.assertTrue(adapter.matches_conversation(sources[0],'user_a'))
            self.assertFalse(adapter.matches_conversation(sources[1],'user_a'))
            cached=[v for (epoch,key),v in adapter._lookup_cache.items() if key==('conversation-batch',)]
            self.assertEqual(len(cached),1)
            self.assertEqual(set(cached[0]),{sources[0].table_name})
            self.assertEqual(adapter.conversation_username(sources[1]),'user_b')
        finally:connection.close()

    def test_batched_public_account_filtered_before_content(self):
        connection=sqlite3.connect(':memory:');connection.execute('CREATE TABLE Name2Id(user_name)')
        connection.executemany('INSERT INTO Name2Id VALUES(?)',[('gh_synthetic',),('ordinary',)])
        connection.execute('PRAGMA query_only=ON')
        shard=SimpleNamespace(path=Path('message_0.db'),connection=connection,shard_name='message_0.db')
        sources=[SourceKey('synthetic-account','synthetic-generation','message_0.db','Msg_'+hashlib.md5(x.encode()).hexdigest()) for x in ('gh_synthetic','ordinary')]
        routes={source.table_name:(shard,) for source in sources}
        adapter=LiveIdentityAdapter(SimpleNamespace(shards=(shard,),_routes=routes,shard_for_source=lambda _:shard),include_public_accounts=False);adapter.begin_scan()
        try:self.assertEqual([adapter.include_source(x) for x in sources],[False,True])
        finally:connection.close()

    def test_sender_cache_rechecks_changed_name2id_and_new_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'message_0.db'
            with sqlite3.connect(path) as writer:
                writer.execute('CREATE TABLE Name2Id(user_name)')
                writer.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(7,?)', ('before',))
            connection = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            table = 'Msg_' + hashlib.md5(b'synthetic').hexdigest()
            source = SourceKey('synthetic-account', 'synthetic-generation', path.name, table)
            shard = SimpleNamespace(path=path, connection=connection, shard_name=path.name)
            catalog = SimpleNamespace(shards=(shard,), shard_for_source=lambda _: shard)
            message = RawMessage(1, 101, 1, 1700000000, 7, 'synthetic', None)
            try:
                adapter = LiveIdentityAdapter(catalog)
                adapter.begin_scan()
                self.assertEqual(adapter.sender_identity(source, message), 'before')
                self.assertEqual(adapter.sender_identity(source, message), 'before')
                with sqlite3.connect(path) as writer:
                    writer.execute('UPDATE Name2Id SET user_name=? WHERE rowid=7', ('after',))
                self.assertEqual(adapter.sender_identity(source, message), 'after')
                adapter.begin_scan()
                self.assertEqual(adapter.sender_identity(source, message), 'after')
            finally:
                connection.close()

    def test_replaced_message_object_is_not_used_for_cached_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'message_0.db'
            with sqlite3.connect(path) as writer:
                writer.execute('CREATE TABLE Name2Id(user_name)')
                writer.execute('INSERT INTO Name2Id VALUES(?)', ('ordinary_username',))
            connection = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            table = 'Msg_' + hashlib.md5(b'ordinary_username').hexdigest()
            source = SourceKey('synthetic-account', 'synthetic-generation', path.name, table)
            shard = SimpleNamespace(path=path, connection=connection, shard_name=path.name)
            catalog = SimpleNamespace(shards=(shard,), shard_for_source=lambda _: shard)
            try:
                adapter = LiveIdentityAdapter(catalog)
                adapter.begin_scan()
                self.assertEqual(adapter.conversation_username(source), 'ordinary_username')
                replacement = path.with_name('replacement.db')
                shutil.copy2(path, replacement)
                replacement.replace(path)
                self.assertIsNone(adapter.conversation_username(source))
            finally:
                connection.close()

    def test_contact_name_missing_or_duplicate_stays_unknown(self):
        connection, catalog, source = _fixture("ordinary_username")
        contacts = sqlite3.connect(":memory:")
        contacts.execute(
            "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT)"
        )
        contacts.executemany(
            "INSERT INTO contact VALUES (?, ?, ?, ?)",
            [
                ("ordinary_username", "first", "", ""),
                ("ordinary_username", "second", "", ""),
            ],
        )
        contacts.execute("PRAGMA query_only=ON")
        try:
            adapter = LiveIdentityAdapter(catalog, contact_connection=contacts)
            adapter.conversation_username(source)
            conversation_id = next(iter(adapter._conversation_names))
            self.assertIsNone(adapter.display_name(conversation_id))
            self.assertIsNone(adapter.display_name("missing-conversation"))
        finally:
            contacts.close()
            connection.close()

    def test_unsupported_contact_schema_does_not_block_base_identity(self):
        connection, catalog, source = _fixture("ordinary_username")
        contacts = sqlite3.connect(":memory:")
        contacts.execute("CREATE TABLE contact (username TEXT, display_name TEXT)")
        contacts.execute("PRAGMA query_only=ON")
        try:
            adapter = LiveIdentityAdapter(catalog, contact_connection=contacts)
            adapter.conversation_username(source)
            conversation_id = next(iter(adapter._conversation_names))
            self.assertEqual(adapter.contact_source_status(), "unsupported")
            self.assertIsNone(adapter.display_name(conversation_id))
            self.assertEqual(adapter.conversation_kind(conversation_id), "私聊或其他（未区分）")
        finally:
            contacts.close()
            connection.close()

    def test_identity_classifies_trusted_sender_and_public_conversation(self):
        username = "gh_example"
        connection, catalog, source = _fixture(username)
        try:
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            message = RawMessage(1, 2, 1, 1_700_000_000, 7, "synthetic", None)
            self.assertTrue(adapter.include_source(source))
            self.assertEqual(adapter.direction(source, message), "inbound")
            conversation_id = next(iter(adapter._conversation_names))
            self.assertEqual(adapter.display_name(conversation_id), username)
            self.assertEqual(adapter.conversation_kind(conversation_id), "公众号")
            self.assertTrue(adapter.evidence().available)
        finally:
            connection.close()

    def test_public_account_filter_is_explicit_and_unknown_identity_stays_unknown(self):
        connection, catalog, source = _fixture("gh_example")
        try:
            adapter = LiveIdentityAdapter(catalog, self_username=None, include_public_accounts=False)
            self.assertFalse(adapter.include_source(source))
            self.assertEqual(adapter.direction(source, RawMessage(1, 2, 1, 1, 99, "synthetic", None)), "unknown")
            self.assertFalse(adapter.evidence().available)
        finally:
            connection.close()

    def test_sender_identity_is_available_even_without_self_username(self):
        connection, catalog, source = _fixture("team@chatroom")
        try:
            adapter = LiveIdentityAdapter(catalog, self_username=None)
            message = RawMessage(1, 2, 1, 1_700_000_000, 7, "synthetic", None)
            self.assertEqual(adapter.sender_identity(source, message), "team@chatroom")
            self.assertEqual(adapter.direction(source, message), "unknown")
        finally:
            connection.close()

    def test_missing_conversation_identity_preserves_base_source_as_unknown(self):
        connection, catalog, source = _fixture("ordinary_username")
        try:
            connection.execute("PRAGMA query_only=OFF")
            connection.execute("DELETE FROM Name2Id WHERE rowid=7")
            connection.execute("PRAGMA query_only=ON")
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            self.assertTrue(adapter.include_source(source))
            self.assertEqual(adapter.conversation_username(source), None)
            self.assertEqual(adapter.conversation_kind("unmapped"), "未知")
        finally:
            connection.close()

    def test_exact_conversation_scope_rejects_other_or_unmapped_sources(self):
        connection, catalog, source = _fixture("team@chatroom")
        try:
            adapter = LiveIdentityAdapter(catalog, conversation_username="team@chatroom")
            self.assertTrue(adapter.matches_conversation(source, "team@chatroom"))
            self.assertFalse(adapter.matches_conversation(source, "other@chatroom"))
        finally:
            connection.close()

    def test_group_classification_uses_exact_hashed_table_mapping(self):
        username = "team@chatroom"
        connection, catalog, source = _fixture(username)
        try:
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            message = RawMessage(1, 2, 1, 1_700_000_000, 8, "synthetic", None)
            self.assertEqual(adapter.direction(source, message), "self")
            conversation_id = next(iter(adapter._conversation_names))
            self.assertEqual(adapter.conversation_kind(conversation_id), "群聊")
        finally:
            connection.close()

    def test_regular_username_is_not_claimed_as_verified_private_chat(self):
        connection, catalog, source = _fixture("ordinary_username")
        try:
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            adapter.direction(source, RawMessage(1, 2, 1, 1_700_000_000, 7, "synthetic", None))
            conversation_id = next(iter(adapter._conversation_names))
            self.assertEqual(adapter.conversation_kind(conversation_id), "私聊或其他（未区分）")
        finally:
            connection.close()

    def test_mentions_without_trusted_self_and_rejected_xml_remain_unknown(self):
        connection, catalog, source = _fixture("team@chatroom")
        try:
            adapter = LiveIdentityAdapter(catalog, self_username=None)
            message = RawMessage(
                1, 2, 1, 1_700_000_000, 7, "synthetic", None,
                raw_source="<msg><atuserlist>other-user,notify@all</atuserlist></msg>",
                source_compression_type=0,
            )
            self.assertEqual(adapter.source_facts(source, message)["mentions"], {
                "status": "available", "at_self": "unknown", "at_all": "yes", "other_count": 1,
            })
            malicious = RawMessage(
                2, 3, 1, 1_700_000_001, 7, "synthetic", None,
                raw_source="<!DOCTYPE msg [<!ENTITY x 'expanded'>]><msg><atuserlist>&x;</atuserlist></msg>",
                source_compression_type=0,
            )
            self.assertEqual(adapter.source_facts(source, malicious)["mentions"], {
                "status": "unknown", "at_self": "unknown", "at_all": "unknown", "other_count": "unknown",
            })
        finally:
            connection.close()

    def test_source_decompression_caps_output_before_accepting_xml(self):
        import zstandard

        connection, catalog, source = _fixture("team@chatroom")
        expanded = b"<msg>" + (b"x" * 2_000_001) + b"</msg>"
        actual = zstandard.ZstdDecompressor()

        class BoundedDecompressor:
            def decompress(self, data, **kwargs):
                self_requested_sizes.append(kwargs["max_output_size"])
                return actual.decompress(data, **kwargs)

        try:
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            for declared_size in (True, False):
                self_requested_sizes = []
                packed = zstandard.ZstdCompressor(
                    write_content_size=declared_size
                ).compress(expanded)
                message = RawMessage(
                    1, 2, 1, 1_700_000_000, 7, "synthetic", None,
                    raw_source=packed, source_compression_type=4,
                )
                with patch("zstandard.ZstdDecompressor", BoundedDecompressor):
                    facts = adapter.source_facts(source, message)["mentions"]
                self.assertEqual(facts["status"], "unknown")
                if not declared_size:
                    self.assertTrue(self_requested_sizes)
                    self.assertLessEqual(max(self_requested_sizes), 2_000_001)
        finally:
            connection.close()

    def test_truncated_zstd_mention_source_is_unknown_for_both_frame_sizes(self):
        import zstandard

        connection, catalog, source = _fixture("team@chatroom")
        xml = b"<msg><atuserlist>sample_self_user,notify@all</atuserlist></msg>"
        try:
            adapter = LiveIdentityAdapter(catalog, self_username="sample_self_user")
            for declared_size in (True, False):
                packed = zstandard.ZstdCompressor(
                    write_checksum=True, write_content_size=declared_size
                ).compress(xml)
                valid = RawMessage(
                    1, 2, 1, 1_700_000_000, 7, "synthetic", None,
                    raw_source=packed, source_compression_type=4,
                )
                self.assertEqual(adapter.source_facts(source, valid)["mentions"], {
                    "status": "available", "at_self": "yes", "at_all": "yes", "other_count": 0,
                })
                for cut in (1, 4):
                    truncated = RawMessage(
                        1, 2, 1, 1_700_000_000, 7, "synthetic", None,
                        raw_source=packed[:-cut], source_compression_type=4,
                    )
                    self.assertEqual(adapter.source_facts(source, truncated)["mentions"], {
                        "status": "unknown", "at_self": "unknown",
                        "at_all": "unknown", "other_count": "unknown",
                    }, f"declared_size={declared_size}, cut={cut}")
        finally:
            connection.close()
