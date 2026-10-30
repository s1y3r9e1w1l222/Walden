# 安装与配置指南

[返回 Walden 使用说明](../README.md)

## 1. 准备条件

- 一台 Mac、Python 3.9 或以上版本，以及 Codex。
- 已适配的 x86_64 消息客户端，以及已同步到本机的一个账号数据目录。其他架构、客户端版本和目录布局不保证可用。
- Apple Command Line Tools，无需安装完整 Xcode。安装命令会处理 Python 依赖；若提示缺少其他系统组件，先按报错补齐。

Walden 读取本机已有消息，不能补回尚未同步或已经缺失的内容。

## 2. 从源码安装

从 [GitHub](https://github.com/s1y3r9e1w1l222/Walden) 下载并解压源码，或使用已克隆的目录。在终端进入包含 `pyproject.toml` 的源码根目录，运行：

```sh
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
APP_HOME="$DATA_HOME/wechat-local"
VENV="$APP_HOME/venv"
test ! -e "$VENV" || { echo "Installation exists; keep it and check before updating." >&2; exit 1; }
mkdir -p "$APP_HOME"
python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install ".[images]"
```

这会建立独立安装环境，并安装基础图片支持。安装目录已存在时会停止，请保留现有安装，不要直接删除重建。任何一步报错时，先处理错误再继续。

## 3. 安装查询技能

安装成功后，在终端运行以下命令，将随包技能复制到 Codex 用户目录。已有同名技能时会停止，不覆盖现有内容。

```sh
SKILL_DIR="$HOME/.agents/skills/wechat-local-query"
test ! -e "$SKILL_DIR" || { echo "Skill exists; keep it and check before updating." >&2; exit 1; }
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
print("Skill installed.")
PY
```

如果 Codex 尚未显示新技能，重启 Codex，再明确使用 `wechat-local-query`。

## 4. 首次配置

在 Codex 中说：

> 帮我配置 Walden 并查询最近一小时消息。

按提示选择本机账号数据目录，并在需要时确认本人身份。配置开始前，保存工作并正常退出消息客户端程序，无需退出账号；如自动打开的窗口需要登录，请本人完成登录。

首次准备可能需要等待，也可以取消。默认先准备最近三小时的消息，可按需要调整；**这不是查询时间上限**。配置成功后再查询；失败或有项目未就绪时，Codex 应说明具体原因，不将其当作全部成功。

## 5. 日常使用

配置完成后，不用每次重新配置。直接在 Codex 中提出请求，例如：

- 看看最近三小时谁找过我。
- 总结项目群今天讨论的重点。
- 列出这段对话里明确提到的待办。
- 根据这段私聊帮我起草回复，先给我确认。

可以指定其他时间范围或单个会话；较大范围按提示确认。消息分类规则可以手动修改，也可以让 Codex 打开可编辑规则，调整摘要、全文等展示方式，下一次查询生效。

## 常见问题

**图片或语音暂时无法解析？**
基础图片支持已随上述命令安装。部分图片格式、语音转写仍需额外的本地组件或模型，可让 Codex 按随包说明协助准备。附件未下载、文件损坏或组件未就绪时，会保留原消息并说明情况，不编造识别结果。

**客户端关闭后还能读吗？**
配置成功后，可以读取已同步到本机的消息；获取后续新消息仍需要客户端完成同步。

**配置失败或没有读到消息怎么办？**
保留错误提示，让 Codex 说明缺少的条件。空结果不一定表示没有消息；数据覆盖不足会明确提示，出现会话归属冲突时会停止相关结果和回复。不要改用其他工程或来源绕过错误。

**会自动发送回复吗？**
不会。实际发送还需要可用的发送组件，并由你明确确认接收对象和完整正文。结果无法确认时，不重复发送同一操作。

请勿把个人配置、消息结果或本地数据上传到公开仓库或问题反馈中。
