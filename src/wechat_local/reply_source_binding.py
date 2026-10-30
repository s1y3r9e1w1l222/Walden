"""Verify declared reply sources against configured stores and live identity."""
import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from .event_normalizer import _standard_id
from .event_store import EventStore, RawMessage, SourceKey, _content_fingerprint
from .live_catalog import SHARD_NAME_PATTERN, TABLE_NAME_PATTERN
from .live_identity import LiveIdentityAdapter, _username_for_table
from .live_readonly import open_encrypted_readonly, read_private_passphrase, readonly_database_uri
from .live_tailer import _table_columns
from .source_setup import load_source_config


class ReplySourceError(ValueError):
    pass


def _stamp(path):
    result = []
    for item in (path, Path(str(path) + '-wal')):
        try:
            info = item.lstat()
            if item.is_symlink() or not item.is_file():
                raise ReplySourceError('source_unavailable')
            result.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
        except FileNotFoundError:
            result.append(None)
    if result[0] is None:
        raise ReplySourceError('source_unavailable')
    return tuple(result)


def verify_reply_sources(config_path, event_ids, target_id, target_label):
    """Verify each declared event in one session; no caller status is trusted."""
    if isinstance(event_ids, str):
        event_ids = (event_ids,)
    if (not event_ids or any(not isinstance(item, str) or not item for item in event_ids)
            or not all(isinstance(x, str) and x and x.strip() == x for x in (target_id, target_label))):
        raise ReplySourceError('source_binding_required')
    event_ids = tuple(sorted(set(event_ids)))
    db = contact = None
    connections = {}
    try:
        config_path = Path(config_path).absolute()
        config_stamp = _stamp(config_path)
        profile = load_source_config(config_path)
        store_path = Path(profile['event_store']).absolute()
        message_root = Path(profile['message_root']).resolve()
        live_root = Path(profile['live_db_root']).resolve()
        contact_path = Path(profile['encrypted_contact_database']).absolute()
        if not message_root.is_relative_to(live_root) or not contact_path.resolve().is_relative_to(live_root):
            raise ReplySourceError('source_unavailable')
        store_before_open = _stamp(store_path)
        db = sqlite3.connect(readonly_database_uri(store_path), uri=True)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        store_version = db.execute('PRAGMA data_version').fetchone()[0]
        store_stamp = _stamp(store_path)
        if store_before_open[0] != store_stamp[0] or db.execute('PRAGMA data_version').fetchone()[0] != store_version:
            raise ReplySourceError('source_unavailable')
        key = read_private_passphrase(Path(profile['passphrase_file']))
        def source_connection(path):
            before_open = _stamp(path)
            canonical = path.resolve(strict=True)
            if canonical not in connections:
                connection, _ = open_encrypted_readonly(path, key)
                try:
                    version = connection.execute('PRAGMA data_version').fetchone()[0]
                    stable = _stamp(path)
                    if before_open[0] != stable[0] or connection.execute('PRAGMA data_version').fetchone()[0] != version:
                        raise ReplySourceError('source_unavailable')
                    connections[canonical] = (connection, stable, version)
                except BaseException:
                    connection.close()
                    raise
            elif before_open != connections[canonical][1]:
                raise ReplySourceError('source_unavailable')
            connection, stable, version = connections[canonical]
            return canonical, connection, stable, version
        _, contact, contact_stamp, contact_version = source_connection(contact_path)
        identity = LiveIdentityAdapter(SimpleNamespace(shards=()), contact_connection=contact)
        if (identity.contact_source_status() != 'configured' or identity._contact_record(target_id) is None
                or identity._contact_display_name(target_id) != target_label):
            raise ReplySourceError('source_unavailable')
        candidates = []
        bindings = []
        checked_tables = set()
        for event_id in event_ids:
            rows = db.execute('''SELECT m.canonical_payload_json,m.payload_sha256,e.event_uid,e.account_id,e.source_generation,
                e.shard_name,e.table_name,e.local_id,r.id AS revision_id,r.revision_number,r.server_id,r.local_type,
                r.create_time,r.real_sender_id,r.content_fingerprint FROM standard_event_materializations m
                JOIN event_revisions r ON r.id=m.source_revision_id
                JOIN events e ON e.id=r.event_id AND e.current_revision_id=r.id
                WHERE m.event_id=? AND e.account_id=? AND e.source_generation=?''',
                (event_id, profile['account_id'], profile['source_generation'])).fetchall()
            if len(rows) != 1:
                raise ReplySourceError('source_unavailable')
            row = rows[0]
            source = SourceKey(row['account_id'], row['source_generation'], row['shard_name'], row['table_name'])
            if not SHARD_NAME_PATTERN.fullmatch(source.shard_name) or not TABLE_NAME_PATTERN.fullmatch(source.table_name):
                raise ReplySourceError('source_unavailable')
            encoded = row['canonical_payload_json'].encode('utf-8')
            if hashlib.sha256(encoded).hexdigest() != row['payload_sha256']:
                raise ReplySourceError('source_unavailable')
            payload = json.loads(encoded)
            conversation = _standard_id('conv_', 'standard-conversation-v1', source.account_id, source.source_generation, source.table_name)
            provenance = payload.get('provenance', {})
            if (payload.get('id') != event_id or payload.get('conversation', {}).get('id') != conversation
                    or provenance.get('raw_event_uid') != row['event_uid']
                    or provenance.get('raw_revision_fingerprint') != row['content_fingerprint']
                    or provenance.get('raw_revision_number') != row['revision_number']):
                raise ReplySourceError('source_unavailable')
            source_path = message_root / source.shard_name
            if source_path.is_symlink() or source_path.resolve().parent != message_root:
                raise ReplySourceError('source_unavailable')
            canonical, connection, source_stamp, _ = source_connection(source_path)
            table_key = (canonical, source.table_name)
            if table_key not in checked_tables:
                if _username_for_table(connection, source.table_name) != target_id:
                    raise ReplySourceError('source_unavailable')
                if 'server_id' not in _table_columns(connection, source.table_name):
                    raise ReplySourceError('source_unavailable')
                checked_tables.add(table_key)
            quoted = '"' + source.table_name + '"'
            source_rows = connection.execute(f'''SELECT local_id,server_id,local_type,create_time,real_sender_id,
                message_content,WCDB_CT_message_content FROM {quoted} WHERE local_id=? LIMIT 2''', (row['local_id'],)).fetchall()
            if len(source_rows) != 1 or _content_fingerprint(RawMessage(*source_rows[0])) != row['content_fingerprint']:
                raise ReplySourceError('source_unavailable')
            candidates.append((source, row['local_id'], row['server_id'], row['local_type'], row['create_time'], row['real_sender_id'], row['content_fingerprint']))
            bindings.append((event_id, row['event_uid'], row['revision_id'], row['content_fingerprint'], conversation, source_stamp[0][:2]))
        store = object.__new__(EventStore)
        store._conn = db
        if store._association_conflict(candidates):
            raise ReplySourceError('conversation_attribution_conflict')
        if (db.execute('PRAGMA data_version').fetchone()[0] != store_version or _stamp(store_path) != store_stamp
                or contact.execute('PRAGMA data_version').fetchone()[0] != contact_version or _stamp(contact_path) != contact_stamp
                or _stamp(config_path) != config_stamp
                or any(connection.execute('PRAGMA data_version').fetchone()[0] != version or _stamp(path) != before
                       for path, (connection, before, version) in connections.items())):
            raise ReplySourceError('source_unavailable')
        binding = [str(config_path), config_stamp[0][:2], store_stamp[0][:2], contact_stamp[0][:2], target_id, target_label, bindings]
        return hashlib.sha256(json.dumps(binding, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
    except ReplySourceError:
        raise
    except Exception as error:
        raise ReplySourceError('source_unavailable') from error
    finally:
        for connection, _, _ in connections.values():
            connection.close()
        if db is not None:
            db.close()


def verify_reply_source(config_path, event_id, target_id, target_label):
    return verify_reply_sources(config_path, (event_id,), target_id, target_label)
