# 安装与配置指南

[返回 Walden 使用说明](../README.md)

## 新用户：安装与首次查询

以下步骤面向 macOS 上受支持客户端的当前本地数据源。既有本机验证不代表其他账号、客户端版本或环境均可成功。先从 GitHub 下载并解压源码，或使用已克隆的源码目录；在终端进入包含 `pyproject.toml` 的源码根目录。不要把数据库、key 文件或查询输出上传，也不要把 key 内容粘贴到对话、配置或命令行。

### 必需准备

| 项目 | 用途与准备方法 | 验证与未就绪表现 |
| --- | --- | --- |
| macOS 与 Python | 当前包声明 Python 3.9+；按下方命令创建独立 venv。本包仅有有限的 macOS 环境验证，其他系统兼容性未知。 | 按安装步骤运行 `"$VENV/bin/python" -m pip check`，应报告无依赖冲突。安装失败时先按错误处理平台/依赖缺口；不要切换系统 Python 或跳过依赖。 |
| 一个完整的当前账号根目录 | 将本机或拷贝来的单个账号目录交给 `wechat-local-setup --account-root`；包按已验证布局识别消息、联系人、会话、图片与附件，并在私有配置中保存路径引用。多账号父目录、缺失资源和越界符号链接会被拒绝或报告。 | 只有 `configured` / `configured_with_voice_incomplete` 才写配置。目录识别本身不等于消息数据库可读，也不证明数据覆盖完整。 |
| 本机消息客户端应用与 LLDB | setup 使用标准 `/Applications/WeChat.app` 和 `xcrun --find lldb`，并在私有配置目录下准备可复用的私有副本。LLDB 可由完整 Xcode 或独立 [Apple Command Line Tools](https://developer.apple.com/documentation/xcode/installing-the-command-line-tools/) 提供，无须安装完整 Xcode。未安装时先按 Apple 说明由本人安装；程序不会代为下载。 | 非指定账号布局、应用或调试器不可用、架构不符、准备或调试失败均不写 ready 配置。相关功能已完成范围内审查与本机验证；其他环境兼容性未证明。 |
| 私有 32 字节 passphrase 文件（仅高级导入） | 只有已持有对应账号凭据的高级用户才需提供 `--passphrase-file`；普通首次接入使用下方 `--acquire-passphrase`。程序不打印凭据内容。 | 捕获值须经全部当前消息分片的 HMAC 只读校验后才私密保存。相关功能已完成范围内审查与本机验证；不代表其他环境兼容。 |
| 私有输出位置与稳定标签 | 为 EventStore、AgentStore 选择两个不同且位于 `LIVE_ROOT` 之外的本地文件路径。`ACCOUNT_ID` 和 `SOURCE_GENERATION` 是本机稳定标签，不是秘密；复用当前源时保持不变，替换来源时换 generation。 | 正式查询会验证路径、各消息分片的凭据及只读打开；`--check-config` 可单独用于排错。首次查询会在两个输出路径创建独立本地存储。输出目录不应位于 live 数据目录内。 |
| 查询时间和分类规则 | 选择明确短时间窗及本机时区。规则是安装的 Markdown 文件，可先复制为用户可编辑覆盖文件。本包只读当前已配置源；不支持旧索引或快照，过去日期也只查询当前源。 | 有界查询返回 coverage/gaps；coverage 不完整或未知必须按部分结果处理，空结果不能证明源中没有其他消息。Skill 应在每次查询前读取当前规则；手动 CLI 参数不会自行解析 Markdown。 |
| 联系人与本人身份（可选） | 若要显示联系人名称，按需提供已核实的 SQLite contact.db 路径；若要判断本人方向/@，仅在你已可信确认本人 username 时设置 `--self-username`。 | 缺联系人库或唯一联系人记录时保留稳定 ID、显示名称为 unknown；未提供可信 username 时本人方向与 `at_self` 为 unknown，不影响原消息。 |

### 从源码安装

在包含 `pyproject.toml` 的源码根目录运行以下命令。它会创建独立虚拟环境并从当前源码安装；默认安装目标已存在时停止，不重置现有环境：

```sh
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
APP_HOME="$DATA_HOME/wechat-local"
VENV="$APP_HOME/venv"
test ! -e "$VENV" || { echo "Venv exists; inspect it before installation" >&2; exit 1; }
mkdir -p "$APP_HOME"
python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install .
"$VENV/bin/python" -m pip check
"$VENV/bin/wechat-local-window" --help
"$VENV/bin/wechat-local-reply" --help
"$VENV/bin/wechat-local-setup" --help
```

如果项目的 GitHub Release 提供 wheel，也可在同一虚拟环境中安装该附件；源码安装不依赖 wheel 附件。

Keep `wechat-local-setup`, `wechat-local-window`, and `wechat-local-reply` in this same virtual environment. If setup returns `entrypoint_missing`, stop and repair this installation; it does not substitute similarly named commands found elsewhere on `PATH`.

### 单账号目录发现与首次凭据接入

通常只提供一个明确的账号根目录：

```sh
ACCOUNT_ROOT="/replace/with/one/account/root"
"$VENV/bin/wechat-local-setup" --account-root "$ACCOUNT_ROOT" --acquire-passphrase
```

首次接入前请保存工作并正常退出桌面消息客户端程序，无须退出账号；必要时由本人在本次私有副本中扫码登录。完成配置后，读取已同步的本地消息无需保持消息客户端运行。程序不会自动退出现有消息客户端。

setup 从本机消息客户端应用准备私有副本，仅对该副本执行 ad-hoc 签名与严格校验；不修改安装版或系统设置。配置尚未成功时，再次运行相同命令会自动复用经核对的同一副本，不重新复制或签名；源版本变化或自有副本缺失时重新准备。未知替换、链接、并发使用或收尾无法确认时停止并保留现场。若明确提供三个目标参数，则使用调用者已准备的目标；已有凭据导入和后续读取不会制作副本或重新取钥。

取钥接线只在严格限定的本机布局尝试：标准 `/Applications/WeChat.app`、本机 `xcrun --find lldb`，以及所选账号根目录正好位于同一 data home 下的 `Documents/xwechat_files/<account>`。账号根在复制目录中、布局不匹配、应用或调试器不可用、权限不允许、断点未命中、捕获key校验失败等情况均失败关闭，不创建 ready 配置；不会扫描其他应用、账号、Keychain 或旧索引，也不会要求用户寻找或输入 key。失败时使用返回的具体状态；不要改填客户端目录来绕过绑定检查。

首次捕获的 `diagnostics` 返回固定阶段、相对耗时、调试器退出码／signal／timeout／cancelled、本次目标 PID 与最后报告状态，以及验证／保存／配置结果。启动使用 `TERM=dumb lldb -s`，`run` 作为普通 LLDB 源脚本命令执行；阶段完成来自断点命令块、命令或进程 API 的实际返回。启动前将该目标进程的标准输出及标准错误定向至 /dev/null，任一设置失败即停止；通过此隔离后可保留有界、脱敏的调试器错误和内部异常以及适用的目标退出码／描述。该隔离仅覆盖目标标准流，不保证阻断其他输出通道或任意调试器插件输出。应用标准日志、原始内存／寄存器、账号路径和凭据不应进入诊断。诊断文件在本次私有目录预创建为0600，按文件身份、归属、会话和调试器 PID 校验，并限制为4096字节。缺失／损坏返回 `capture_diagnostic_invalid`，未完成或失败阶段返回 `capture_stage_failed`；除下述限定的首次启动后继处理外均停止，不产生ready配置。调试器非零退出始终返回 `debugger_failed`，即使已写捕获文件也不处理该值。

`target_exit=intentional` 表示本脚本的终止调用成功且观察到目标退出；`unexpected` 表示在主动终止前已观察到退出或崩溃；否则为 `unknown`。`group_cleanup` 与 `target_cleanup` 分别记录进程组与目标的确认状态，组清理成功不能证明逃逸目标已退出；超时的目标收尾仍为unknown。PID和状态只是本次 LLDB 观察，不能用作后续终止其他进程的依据。诊断不证明真实客户端采用了指定账号路径，不推断签名或权限根因，也不代表本包已重新完成真实首配。

默认自动副本首次启动前，程序有限度地观察本机系统的扫描事件，只在内存保留必要的关联状态，不保存原始日志。若首次确认为特定的调试器附加等待失败，且断点和捕获未进入、目标及进程组收尾确认，本次目标 PID 的可识别排队事件、同一 PST 的扫描结束记录及当前副本的精确 URL 均匹配，程序会再次核对归属并自动使用同一副本接续一次，不重新复制或签名，也不循环。`diagnostics.policy_observation` 记录观察状态和后继状态，`diagnostics.attempts` 保留两次诊断。首错保留，第二次失败照常停止。首次失败结束时若没有本次 PID 到排队 PST 的可识别关联，立即报告观察不可用；已关联但尚无 URL 或完成记录时才继续等待。系统日志格式不同、权限不足、内容被遮蔽、进程退出、检测到日志丢失或证据不匹配时不能据此重启；其他失败、未知目标收尾、取消及显式目标也不触发。观察到一次扫描结束不代表全部系统策略已经完成，或下一次附加一定成功；其他环境是否具备这些条件仍需实际确认。

默认可等待捕获或扫描观察，直到完成或本人按 Ctrl-C 取消；`--capture-timeout` 可指定最多300秒的正数，其同一个截止时间从首次 LLDB 启动前开始，覆盖首次调试、观察等待及唯一后继启动；复制、签名、分片校验以及密钥和配置保存不在此限时内。0 表示不限外层等待，但最多只接续一次；它不能延长调试器内部约30秒的附加等待。若没有可识别的完成证据且观察仍可用，程序不会把安静或经过某个固定秒数当成完成；可以 Ctrl-C 取消。等待取消和超时分别报告 `scan_wait_cancelled` / `scan_wait_timeout`，原首次失败诊断仍保留。

程序在独占私有暂存目录中调用调试器，持续排空输出但不保存应用日志。捕获值必须恰好32字节，并通过选中账号下每个当前消息分片的既有SQLCipher HMAC只读验证，之后才会以0600排他保存并继续原setup。超时报告`failure_reason=debugger_timeout`；若无法确认目标生命周期清理，外层状态为`debugger_cleanup_uncertain`。程序只终止本次创建且可确证归属的调试器进程组。只有捕获文件仍是本次创建的 inode 时才删除它；如果文件被替换或清理状态无法读取，会在`cleanup_state`中明确报告，并保留私有暂存状态，不写入ready配置，也不声称捕获文件已移除。

相关功能已完成范围内审查与本机验证；不证明其他账号、客户端或环境兼容。静态目录结构匹配不证明应用实际使用指定 data home；复制到其他位置的账号树目前无法自动绑定。若未返回 configured 状态，不要继续查询。

仅当你已经持有对应当前账号的私有 32 字节 key 文件时，才可使用高级验证分支：

```sh
PASSPHRASE_FILE="/replace/with/an/already-existing/private/key-file"
"$VENV/bin/wechat-local-setup" --account-root "$ACCOUNT_ROOT" --passphrase-file "$PASSPHRASE_FILE"
```

只有现有 key 通过消息分片只读打开验证后，setup 才会写入私有 source 配置。语音 DB 由账号根下有限深度的 schema-only 检查发现；不完整扫描会留下语音 unavailable 状态，但不阻断基础文本。若固定图片路径及同级 `app_data` 命名来源可唯一确认 V2 key 参数，参数写入独立 0600 私有文件；否则 source config 将图片凭据记为未配置，已发现资源路径仍可用于非 V2 图片，V2 图片按原 decoder 返回不可用。

基础配置成功后，setup 默认通过同安装的窗口入口扫描最近3小时建立基线；可在首次 setup 加 `--baseline-request '最近1小时' --baseline-timezone 'Asia/Shanghai'` 等受支持的窗口语法和明确的 IANA 时区调整。自选请求缺少或提供无效时区时会在取钥和保存配置前拒绝。大范围仍遵守窗口入口原有确认要求。结果分别报告 `message_baseline`、`self_identity` 和 `mute_status`；基线失败或取消不会撤销成功配置，取消返回退出码130并报告基线未完成。基线状态保留实际 coverage/gaps；当前仅对来源未变且能证明为空的表复用覆盖，非空表仍实时读取。首次基线检查各表有效时间字段（可含选定窗口外的时间元数据），只保存必要布尔证明，不保存窗口外具体时间或正文；首次尝试失败或取消后，后续普通查询不再自动重复此全表时间探测，变化或新增来源仍按请求窗口回读。已确认的群聊来源中，整数通知值0映射 `muted`、1映射 `unmuted`；仅稳定会话标识以 `@chatroom` 结尾且联系人来源可验证时适用。私聊及缺失、歧义、来源变化或其他值仍为 unknown；每次查询据当前 `source_facts` 应用已有规则，未知时说明规则可能未完全适用，不把 unknown 当作未静音。首配整体静音状态仍为 incomplete，不代表所有会话已确认。

如果自选大窗返回 `confirmation_required`，基础配置仍保留；用同安装的日常 window 入口按原有大窗确认流程继续，不会自动绕过确认。

默认位置 `$HOME/.local/share/wechat-local/venv`（或设置 `XDG_DATA_HOME` 后的 `$XDG_DATA_HOME/wechat-local/venv`）供 Skill 从其他工作目录定位同一安装。shell 激活或变量不会传递给 Codex 桌面/其他终端；若选择其他位置，须将实际解析出的绝对 `runtime_python`、`window_cli`、`reply_cli` 三条命令路径写入下方用户配置记录。用 `"$VENV/bin/python" -c 'import sys; print(sys.executable)'` 可核对实际 Python 绝对路径。后续每个终端步骤都从 XDG/HOME 重新构造此位置，不依赖激活环境。

安装包仍包含 `wechat_local/resources/tools/render_lldb_capture.py`，供本机接线诊断与复核；首次配置通过上面的setup入口调用它，不需用户手工生成命令或读取key。若你已经持有经授权、属于该账号的私有key文件，也可显式使用下方高级 `--passphrase-file` 分支；文件形状合格仍须由setup对全部当前消息分片只读验证。

安装包内的查询 Skill 与默认规则是独立资源。将 Skill 安装到 Codex 标准用户目录 `~/.agents/skills/wechat-local-query/`；目标已存在时先检查并保留已有内容，不覆盖。可用以下命令从已安装包复制 Skill（目标必须不存在）：

```sh
SKILL_DIR="$HOME/.agents/skills/wechat-local-query"
test ! -e "$SKILL_DIR" || { echo "Skill exists; inspect it before updating" >&2; exit 1; }
mkdir -p "$SKILL_DIR"
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
PYTHON="$DATA_HOME/wechat-local/venv/bin/python"
"$PYTHON" - <<'PY'
from importlib.resources import files
from pathlib import Path

target = Path.home() / ".agents/skills/wechat-local-query/SKILL.md"
resource = files("wechat_local").joinpath("resources/skills/wechat-local-query/SKILL.md")
target.write_bytes(resource.read_bytes())
target.chmod(0o600)
print(f"Installed Skill file at {target}")
PY
```

Codex 用户级 Skill 目录及发现规则见[官方 Skills 文档](https://learn.chatgpt.com/docs/build-skills)。安装文件不等于当前会话已加载；若新 Skill 未出现，重启 Codex 后在新回合显式调用 `wechat-local-query`。本包不会改全局模型或项目指针。

### 跨终端使用自动生成的来源引用

首次取钥成功或现有凭据验证成功后会创建私有 `current-source.json`，其中记录解析出的运行时 Python、window/reply CLI、单一账号来源和本机输出路径引用；不保存 key 正文。正常查询通过公开 `wechat-local-window --source-config <current-source.json>` 参数读取该配置。该 CLI 已有对应配置解析入口，Skill 不手写第二份路径/账号 JSON，也不靠 shell 激活状态、推测 venv 或扫描账号。

查询前应检查 `$XDG_CONFIG_HOME/wechat-local/current-source.json`，或在 `XDG_CONFIG_HOME` 未设置时检查 `~/.config/wechat-local/current-source.json`。setup 因凭据缺失以 exit 2 结束时不会创建该文件；不要手动伪造 ready profile。若使用自定义安装位置，setup 生成的 profile 记录实际绝对 CLI 路径，Skill 从该 profile 读取并核对命令是否存在。

第一次建立可编辑规则覆盖（若文件已存在，保留并编辑原文件）：

```sh
RULES_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/wechat-local"
mkdir -p "$RULES_DIR"
if [ ! -e "$RULES_DIR/WA_MESSAGE_CLASSIFICATION_RULES.md" ]; then
  DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
  PYTHON="$DATA_HOME/wechat-local/venv/bin/python"
  "$PYTHON" - <<'PY'
import os
from importlib.resources import files
from pathlib import Path

config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
target = config_home / "wechat-local/WA_MESSAGE_CLASSIFICATION_RULES.md"
resource = files("wechat_local").joinpath("resources/docs/WA_MESSAGE_CLASSIFICATION_RULES.md")
target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
if target.exists():
    raise SystemExit("Rules file exists; refusing to overwrite")
target.write_bytes(resource.read_bytes())
target.chmod(0o600)
print(f"Edit rules at {target}")
PY
fi
```

Skill 在下次查询前读取该用户覆盖；直接运行 CLI 时，应自己按规则选择现有参数，例如公众号纳入使用 `--include-public-accounts`，不纳入则省略。无需重新安装。

### 首次确认本人身份并复用

“本人”使用稳定的消息客户端内部 username 唯一标识，昵称、备注、目录名和图片参数都不能证明归属。当前包没有经证明的自动自识别来源；Skill 会优先复用用户已明确确认的可信标识，否则首次请用户确认一次，再调用正式入口保存。用户无需手改 JSON，也无需每次查询重新填写。

已有来源配置时，Skill 在用户确认后运行以下命令（`SELF_USERNAME` 表示这次明确确认的唯一标识，不是昵称）：

```sh
CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
CONFIG="$CONFIG_HOME/wechat-local/current-source.json"
"$VENV/bin/wechat-local-setup" --config "$CONFIG" --confirm-self-username "$SELF_USERNAME"
```

仅返回 `self_identity_confirmed`（exit 0）才表示保存成功。此入口不读取密钥文件、不打开数据库、不重新取钥；其余配置字段保持原值。新建来源配置时，也可在既有 setup 命令后加 `--confirm-self-username "$SELF_USERNAME"`，随已验证来源一起保存。确认命令不回显本人标识；不要把真实标识、私有配置或查询结果复制到日志、issue 或公开示例。由 Skill 直接传参，无需用户在交互 shell 中留下真实值。

身份保存在本机 0600 的 `current-source.json`，绑定该配置的账号标识、来源代次和规范的数据库／消息根路径。配置本身及其父路径不能经过符号链接，更新入口要求所在目录私有；请使用规范路径。后续直接运行原有 `wechat-local-window --source-config "$CONFIG" ...`，自动恢复本人身份。昵称或备注改变不改变归属。没有身份字段的旧配置仍可读完整原文，收发方向和 `at_self` 保持 `unknown`；未经确认不猜测。

明确更正本人或在来源变更后重新确认时，Skill 先说明当前选择并取得这一次更正确认，再调用：

```sh
"$VENV/bin/wechat-local-setup" --config "$CONFIG" --confirm-self-username "$SELF_USERNAME" --replace-self-identity
```

`--replace-self-identity` 仅用于现有配置的身份更正／改绑，不和 `--account-root`、取钥或密钥导入参数混用。没有明确更正时，不静默覆盖已保存身份。账号、代次或规范来源路径与绑定不符时，查询返回 `configuration_detail=self_identity_binding_mismatch`；显式 `--self-username` 和已保存值冲突时返回 `self_identity_conflict`，并提示重新确认。同值显式参数和没有保存身份的高级显式调用仍兼容。非法标识、符号链接、不安全权限、配置变更或保存失败不会返回确认成功；替换前发生失败保留原配置。绑定用于检测不一致，并非防止本机同一用户同时改写全部配置的认证机制。

本人身份功能已完成范围内审查与验证；实际账号身份效果仍需按来源事实核对。系统消息、`@所有人` 和本人发言仍以原有事实／原文保留，当前模型按分类规则解释，不用删除本人原文实现“谁找我”。

### 有界查询

只有 setup 已成功写入来源配置时才可执行查询。CLI 通过 `--source-config` 使用该配置；不需要手动设置数据库路径、key 路径或本机标签。默认安装位置可从 XDG/HOME 重建；自定义位置使用 profile 中记录的 `window_cli` 绝对路径。缺少配置或 setup 返回任何非 configured 状态时停止，不改用显式路径或旧索引：

```sh
CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
CONFIG="$CONFIG_HOME/wechat-local/current-source.json"
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
WINDOW_CLI="$DATA_HOME/wechat-local/venv/bin/wechat-local-window"
test -x "$WINDOW_CLI" || { echo "Inspect the installed window_cli path in current-source.json" >&2; exit 1; }
```

直接按用户指定的明确短时间窗查询；查询本身会验证配置、凭据和消息分片的只读打开，复用这些连接读取消息。示例时间只说明语法：

```sh
"$WINDOW_CLI" --source-config "$CONFIG" \
  --request "从 2025-01-01 09:00:00 到 2025-01-01 09:05:00" \
  --timezone "Asia/Shanghai" --json
```

如需单独排查配置，可运行 `"$WINDOW_CLI" --source-config "$CONFIG" --check-config --json`；它是独立诊断，不是日常查询前置。

首次查询会读取并输出该窗口消息正文；请在本机查看，不要把原始 JSON 上传。错误 key、坏权限、缺失分片或覆盖不完整时只报告受控错误/缺口，不转查旧索引。出现相关失败时，JSON 的 `diagnostics.failures` 给出受控阶段和原因；未知原因记为 `unknown`，不包含原始异常、数据库路径或消息。语音 JSONL 的完成事件也会报告读取后发生的关联失效。运行命令可从任意 cwd 使用 `$WINDOW_CLI` 的绝对路径，不依赖 activate。

### 按需启用的可选功能

- **图片**：单目录 setup 自动定位固定来源布局的 resource DB、附件根和 sibling `app_data` 图片参数，并将 `key_codes/account_scope` 写入私有配置文件；其来源目录缺失、改名或存在歧义时，V2 解密不可用，但原消息保留且非 V2 解码仍可尝试。普通显式参数调用可用 `--image-database`、`--image-attachments-root`、`--image-output-root`；V2 可通过 `--image-key-config` 指定私有 JSON。wxgf/HEVC 还需 `--image-ffmpeg`。可选 `images` extra 安装 Pillow/cryptography。HEVC 首帧解码已完成范围内验证；其他环境与格式仍需单独验证。
- **语音**：setup 在单账号 `db_storage` 内有限深度发现 `VoiceInfo`/`Name2Id` schema；只有扫描完整后 profile 才记录 voice DB 列表。启用语音仍需 `--voice-transcripts` 和独立本地 ASR runtime/Python；安装包不含 pilk、whisper.cpp 或模型。按安装包中 `wechat_local/resources/docs/VOICE_RUNTIME.md` 与 `install_local_voice_runtime.sh` 安装固定来源的独立依赖。任一 shard 缺失或错误 key 时语音 enrichment fail closed 为 `voice_source_unavailable`，原消息仍输出且不应启动 ASR。本地转写功能已完成范围内验证；这不代表识别准确率或其他账号、环境的覆盖。

例如在源码根目录将图片 extra 加到相同虚拟环境：

```sh
"$VENV/bin/python" -m pip install '.[images]'
```

已准备媒体时，使用同一安装的 `window_cli --source-config <current-source.json>` 查询：图片加 `--configured-images`，首次使用先按 `images` extra 安装 Pillow/cryptography；wxgf/HEVC 另加 `--image-ffmpeg <本机可执行文件>`。若要本地语音识别，按随包 `VOICE_RUNTIME.md` 准备 runtime；setup 生成的配置中，`event_store` 的父目录即 `<app-home>`，其文档约定的 runtime 位置为 `<app-home>/voice-runtime`。然后同时加 `--voice-transcripts --voice-jsonl --voice-runtime-root <已准备的runtime目录> --voice-python <配置中的runtime_python>`。配置已包含发现的语音库，不要与 `--voice-database` 显式参数混用；runtime 未准备或解析失败时保留原消息并如实报告，不把缺失依赖描述为媒体不存在。

- **确认后发送**：需 Codex 能加载 `computer-automation` Skill/Midscene，并由用户核对精确稳定目标和完整正文后明确确认。发送结果未知时不得重试该操作。运行 `wechat-local-reply prepare` 时提供同一配置 `--source-config` 和建议所依据的每个 `--source-event-id`（多条时重复该参数），包会核验当前来源及目标；只有确认该目标和正文后才运行返回 operation 的 `confirm`。旧格式待确认操作须重新准备并确认。这不是首次只读查询的前置；其他设备和环境仍需单独验证。

## Current entry point

This first-version package supports bounded reads from an already configured current messaging client source.
It reads only the configured current source and does not fall back to other data sources when coverage is unavailable. A requested past date may return incomplete coverage.

Install the package in a Python 3.9+ virtual environment. Use `wechat-local-setup --account-root ... --acquire-passphrase` to prepare the private copy and acquire a credential, or use the advanced existing-key import. Failure does not create a ready config. The advanced existing-key branch writes `current-source.json` only after all current message shards open read-only. Queries then use `wechat-local-window --source-config <current-source.json>` with a bounded time window and timezone. EventStore and AgentStore files are created privately on first query, separate from the live database root. Coverage reports known missing sources and leaves off-window timestamp integrity unknown; do not treat an empty result as proof that undiscovered data is absent.

Use `--conversation-username` to bind a query to one exact `Name2Id.user_name`; it is checked before preflight sampling or message-content reads. Without it, queries remain account- and time-bounded. Unknown identity keeps the original text labeled unknown; media enrichment is withheld when its exact source binding is unavailable. For windows longer than 24 hours, JSON output includes a content-free `confirmation` with a fixed absolute local-time request and token. Rerun with that exact request and `--confirmation-token <token>`; a changed source estimate invalidates the token and stops the query.

The package declares pinned runtime dependencies and optional image dependencies. The package does not modify a user's global Python installation.

Do not put credentials, database files, user paths or real query output in issue reports. The package only reads user-authorized local data. Start with synthetic fixtures and a narrowly specified time range. Confirm the stable self username once with `wechat-local-setup --config <private-config> --confirm-self-username <confirmed-username>` (or add that option during initial setup). The mode-0600 profile binds it to the account, generation and canonical database/message roots; later `--source-config` queries restore it. Updating the identity does not read the key or databases. Explicit correction/rebinding additionally requires `--replace-self-identity`. A mismatched binding or conflicting `--self-username` fails with an actionable identity error; legacy profiles without a saved identity retain unknown direction and `at_self`. The optional explicit `--self-username` remains available for advanced calls. With a trusted identity, direction and conversation username are resolved only through the exact message-table hash and `Name2Id.rowid` lookup. `@chatroom` and `gh_` naming rules label group/public-account conversations; other usernames remain “private or other, unclassified,” not proven private. An optional `--contact-database` supports an explicitly selected readable SQLite contact source. Alternatively, `--encrypted-contact-database` selects one encrypted contact database under `--live-db-root`; it uses the already verified passphrase read-only and does not discover or scan other contact files. Both forms require the evidenced `contact(username, alias, remark, nick_name)` schema and look up only exact stable usernames relevant to returned messages. The output retains raw `chat_room_notify`, `verify_flag` and contact `local_type` values as source facts. For a verified `@chatroom` only, an exact integer `chat_room_notify` of 0 means `mute_status=muted` and 1 means `unmuted`; private chats, missing or ambiguous contacts, changed sources and all other types or values remain `unknown`. Other raw fields do not determine subtype or mute status. Bounded message source XML may separately report `at_self`, exact `notify@all`, and a count of other mention tokens; it never exposes the token list. Missing, duplicate, malformed, oversized or unsupported facts remain unknown while the original message is preserved. It preserves each stable `sender_identity` alongside its optional `sender_display_name`.

## Classification rules

The installed default is `wechat_local/resources/docs/WA_MESSAGE_CLASSIFICATION_RULES.md`. To customize it, copy that file to `$XDG_CONFIG_HOME/wechat-local/WA_MESSAGE_CLASSIFICATION_RULES.md` (or `~/.config/wechat-local/WA_MESSAGE_CLASSIFICATION_RULES.md` when unset) and edit the copy. The Skill reads that current override before each query, falling back to the installed default if no override exists. It maps public-account inclusion to `--include-public-accounts` and only enables configured image/voice options when current rules or the current request require attachment parsing. Changes take effect on the next query without code changes or reinstalling. The CLI does not parse Markdown rules or infer policies itself.

## Voice

Voice enrichment is opt-in. The query prints the base message record before emitting separate voice events. Every explicitly selected voice shard must open successfully before a match can be considered complete. A missing, unreadable or wrong-key shard produces `voice_source_unavailable`; it does not authorize an ASR attempt. Local ASR requires the separately installed runtime described by the installed package resource `wechat_local/resources/docs/VOICE_RUNTIME.md`, including hash-checked `pilk`, whisper.cpp and model; these are not included in the package. The supplied helper is at `wechat_local/resources/tools/install_local_voice_runtime.sh`. Voice transcription has received scoped validation; recognition accuracy is not guaranteed.

Group and private voice identity association use exact `Name2Id` and message binding. Broad private-chat coverage or clean-install ASR is not guaranteed.

## Images

An opt-in low-level image adapter is included and wired into the one-shot query CLI with `--image-database`, `--image-attachments-root` and `--image-output-root`. It binds an exact message identity to one resource record, then resolves each distinct `MessageResourceDetail.size` to at most one attachment whose token and byte size both match. Each verified representation is returned separately; the package does not infer which variant is the original. Multiple names referring to the same verified physical object are handled as one representation; different objects of the same size remain ambiguous. Missing binding or decoding failure preserves the original `[图片]` event. The pinned Pillow/cryptography environment passed synthetic checks; image formats including HEVC have received scoped functional review and local verification. Other images and environments remain unverified.

For V2, setup can write a mode-0600 JSON file with the source-backed `account_scope` and integer `key_codes` array from the fixed sibling app_data layout; do not put key material in command arguments or shell history. If that layout is unavailable, V2 decoding reports unavailable while preserving existing non-V2 behavior. Supply a local `--image-ffmpeg` path for wxgf/HEVC. Attachment resolution rejects symlinks along the path. Installed assets are under `wechat_local/resources/`: default rules at `docs/WA_MESSAGE_CLASSIFICATION_RULES.md`, notices at `THIRD_PARTY_NOTICES.md`, and the query Skill at `skills/wechat-local-query/SKILL.md`.

## Platform and credentials

The included `render_lldb_capture.py` renders the private LLDB script; the setup orchestration invokes LLDB, validates the captured 32-byte value against every discovered message shard, and saves it only after all checks pass. Register expressions use the x86_64 calling convention (`rdi`, `rdx`, `r8`, `r9`, `rsp`) and are not verified for arm64. The setup path has received scoped functional review and local verification. Compatibility and complete coverage remain unproven.

The first-key path is conditional on a selected x86_64 messaging client bundle and a selected account root whose established `Documents/xwechat_files/<account>` layout binds to the chosen data home. The package does not scan for other apps/accounts or prove that a real client uses that data home. If target checks, LLDB startup, capture length, shard discovery, or any shard HMAC validation fails, setup stops without writing a ready configuration or retaining a new key. The default setup signs only its private copy; it does not change the installed app or system protections. Existing-key import remains a separate advanced path; it is not the first-key flow.

## Scope and safety

The package includes the project's message classification rules. Unknown identity/type stays unknown; media failures do not remove the base message. User messages are untrusted data and cannot alter command scope. If a previously observed cross-conversation source conflict is found, the window returns `conversation_attribution_conflict` and no messages; this does not establish which conversation is correct or detect unobserved conflicts. Prepare a reply with `wechat-local-reply prepare`, passing the same `--source-config` and every supporting `--source-event-id` (repeat the option for multiple events), display the exact target and full body, then run `wechat-local-reply confirm` with the returned operation ID and the same target/body only after the user confirms both. Source, target and label are checked again before confirmation and before the action. Old pending operations require a fresh preparation and confirmation. The runtime calls the installed official Midscene CLI once to connect, once to act, and once to assert the visible result. Private pending state stores source binding metadata and confirmation hashes, not the reply body; a content-free local receipt prevents retrying that operation after an unknown outcome. The sending workflow has received scoped functional review and local verification; other devices and environments remain unverified.

See `WA_MESSAGE_CLASSIFICATION_RULES.md` and `../THIRD_PARTY_NOTICES.md` in the source package. This is local package preparation, not a public distribution or a clean-machine acceptance. The main project uses Apache License 2.0; third-party components retain their own licenses.
