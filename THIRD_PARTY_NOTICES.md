# Third-party notices

`src/wechat_local/wcdb_crypto.py` 中的 SQLCipher4/WCDB 第一页 HMAC 校验逻辑改编自
[`CloudDreamAI / TANGandXUE wcdb-key-tool`](https://github.com/TANGandXUE/wcdb-key-tool)。
原项目采用 MIT License；许可证全文保存在
`licenses/wcdb-key-tool.LICENSE`。

本仓库没有迁入第三方工具的进程扫描、调试器控制或逐页解密代码。当前主链路只保留已经
在本项目中实际使用和验证过的密码参数、PBKDF2 派生及第一页 HMAC 校验逻辑，数据库导出
仍由 `sqlcipher3` / SQLCipher 4 完成。

