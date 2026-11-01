# File attachment references

The bounded window command can locate local files referenced by visible, attributed file messages. This is opt-in enrichment of an existing EventRef; it does not download, open for content interpretation, extract archives, evaluate macros, or execute files. It does not add machine-local paths to Standard Event payloads or stores.

Use `wechat-local-window` with a bounded request, an existing source configuration, `--file-attachments-root /absolute/canonical/selected-account/file-root`, and `--json`. The explicitly selected root must belong to the same account and contain no symlink components. Without the option, no file enrichment runs. Only resolved results expose a path; revalidate the reference before later use because files may change.

The resolver reads bounded XML metadata from the exact raw message revision. A verified adapter's explicit resource binding takes priority when supplied; otherwise filename candidates require the message MD5 and, when present, the matching size. Only direct `appmsg/appattach/md5` and `appmsg/md5` are accepted; conflicting direct values reject the metadata, and nested thumbnail hashes are ignored. Name, time, or size alone do not prove identity. Multiple matching paths remain ambiguous. A missing candidate does not establish whether a file was ever downloaded. MD5 is correlation evidence, not adversarial authenticity.

| Status | Meaning |
| --- | --- |
| resolved | Unique verified local file reference |
| not_downloaded | Verified adapter explicitly reports not downloaded |
| missing | Verified resource path is absent |
| not_found | No candidate; never-downloaded versus deleted is unknown |
| ambiguous | Several paths satisfy identity checks |
| insufficient_evidence | No message hash or verified resource path |
| verification_failed | Size, hash or signature mismatch |
| unsupported_format | Extension is outside the supported list |
| invalid_metadata / not_file | Malformed or oversized XML, or a different message type |
| unsafe_path / unavailable / scan_incomplete | Unsafe reference, source failure/change, or incomplete bounded scan |

Supported extensions are `.pdf`, `.xlsx`, `.zip`, `.doc`, `.docx`, `.docm`, `.wps`, `.xls`, `.xlsm`, `.xltm`, `.numbers`, `.pptx`, `.rar`, and `.7z`. Format checks inspect ZIP/OLE/PDF/RAR4/RAR5/7Z signatures only, not file contents. Unsupported signatures, encrypted formats, or misleading extensions can return `verification_failed/format_mismatch`.

A resolution visits at most 20,000 entries, depth 8, 100 filename candidates, 256 MiB per file and 512 MiB of candidate hashing. It does not follow symlinks. Directory snapshots enclose enumeration and hashing; file identity, size, modification time and change time are checked around reads and again before returning a unique path. Resolved references contain SHA-256 for later safe reopening and revalidation. Source attribution is checked before results are emitted.

No verified file-resource adapter is currently available. The optional resource-binding API represents evidence from an independently verified adapter; it does not establish that the current client exposes such an association. In its absence, download status remains unknown where no candidate is found.
