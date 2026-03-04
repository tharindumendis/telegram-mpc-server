# tgcli — Telegram MCP Server (stdio fork)

> **Fork of [kfastov/tgcli](https://github.com/kfastov/telegram-mcp-server)** with added native **stdio transport** for MCP — enabling direct integration with AI orchestrators like LangGraph, Claude Desktop, Cursor, and any agent that uses the stdio MCP protocol.

Telegram CLI with background sync and a full MCP server for your personal account (MTProto, not bot API).

## What's new in this fork

- ✅ **`tgcli mcp --transport stdio`** — native stdio MCP server (zero HTTP overhead)
- ✅ **`mcp-stdio.js`** entry-point — can be called directly with `node` or via `npx`
- ✅ Credentials from environment variables (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`)
- ✅ Compatible with any MCP stdio client (LangGraph, Claude Desktop, Cursor, etc.)

---

## Installation

```bash
npm install -g @tharindumendis100/tgcli
```

Or use without installing (via npx — auto-downloads):

```bash
npx -y @tharindumendis100/tgcli mcp --transport stdio
```

---

## Authentication

Get Telegram API credentials:

1. Go to https://my.telegram.org/apps
2. Log in with your phone number
3. Create a new application
4. Copy `api_id` and `api_hash`

Then authenticate once (saves your session locally):

```bash
tgcli auth
```

> **Note:** You only need to run `tgcli auth` once per machine. After that, the stdio server resumes from the saved session automatically — no interactive login needed.

---

## Quick Start

```bash
tgcli auth
tgcli sync --follow
tgcli messages list --chat @username --limit 20
tgcli messages search "course" --chat @channel --source archive
tgcli send text --to @username --message "hello"
tgcli server
```

---

## MCP stdio Server (NEW)

This is the primary new feature of this fork. Use it with **any AI agent that supports MCP stdio**.

### Option 1 — via npx (no install required)

```bash
npx -y @tharindumendis100/tgcli mcp --transport stdio
```

### Option 2 — via global install

```bash
tgcli mcp --transport stdio
```

### Option 3 — via direct node call

```bash
node /path/to/mcp-stdio.js
```

---

## Agent / Orchestrator Config

Add this block to your agent's `config.yaml` or MCP client config:

```yaml
mcp_clients:
  - name: "telegram"
    command: "npx"
    args: ["-y", "@tharindumendis100/tgcli", "mcp", "--transport", "stdio"]
    env:
      TELEGRAM_API_ID: "your_api_id"
      TELEGRAM_API_HASH: "your_api_hash"
```

For **Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "telegram": {
      "command": "npx",
      "args": ["-y", "@tharindumendis100/tgcli", "mcp", "--transport", "stdio"],
      "env": {
        "TELEGRAM_API_ID": "your_api_id",
        "TELEGRAM_API_HASH": "your_api_hash"
      }
    }
  }
}
```

For **Cursor** (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "telegram": {
      "command": "npx",
      "args": ["-y", "@tharindumendis100/tgcli", "mcp", "--transport", "stdio"],
      "env": {
        "TELEGRAM_API_ID": "your_api_id",
        "TELEGRAM_API_HASH": "your_api_hash"
      }
    }
  }
}
```

---

## MCP HTTP Server (original)

The original HTTP transport is still fully supported:

```bash
tgcli config set mcp.enabled true
tgcli config set mcp.host 127.0.0.1
tgcli config set mcp.port 8080
tgcli server
```

Then point your client at `http://127.0.0.1:8080/mcp`.

---

## All Commands

```bash
tgcli auth           Authentication and session setup
tgcli config         View and edit config
tgcli sync           Archive backfill and realtime sync
tgcli mcp            Start MCP server (--transport stdio | http)  ← NEW
tgcli server         Run background sync service (HTTP MCP optional)
tgcli service        Install/start/stop/status/logs for background service
tgcli channels       List/search channels
tgcli messages       List/search messages
tgcli send           Send text or files
tgcli media          Download media
tgcli topics         Forum topics
tgcli tags           Channel tags
tgcli metadata       Channel metadata cache
tgcli contacts       Contacts and people
tgcli groups         Group management
tgcli doctor         Diagnostics and sanity checks
```

Use `tgcli [command] --help` for details. Add `--json` for machine-readable output.

---

## Available MCP Tools

Once connected, the following tools are exposed to your AI agent:

| Category     | Tools                                                                                                                                                               |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Channels** | `listChannels`, `searchChannels`, `listActiveChannels`, `listTaggedChannels`, `setChannelTags`, `listChannelTags`, `autoTagChannels`                                |
| **Metadata** | `getChannelMetadata`, `refreshChannelMetadata`                                                                                                                      |
| **Topics**   | `topicsList`, `topicsSearch`                                                                                                                                        |
| **Messages** | `messagesList`, `messagesGet`, `messagesContext`, `messagesSearch`, `messagesSend`, `messagesSendFile`                                                              |
| **Media**    | `mediaDownload`                                                                                                                                                     |
| **Contacts** | `contactsSearch`, `contactsGet`, `contactsAliasSet`, `contactsAliasRemove`, `contactsTagsAdd`, `contactsTagsRemove`, `contactsNotesSet`                             |
| **Groups**   | `groupsList`, `groupsInfo`, `groupsRename`, `groupsMembersAdd`, `groupsMembersRemove`, `groupsInviteLinkGet`, `groupsInviteLinkRevoke`, `groupsJoin`, `groupsLeave` |
| **Sync**     | `scheduleMessageSync`, `getSyncedMessageStats`, `listMessageSyncJobs`                                                                                               |

---

## Configuration & Store

The tgcli store lives in the OS app-data directory and contains `config.json`, sessions, and `messages.db`.  
Override the location with `TGCLI_STORE`.

Legacy version: see `MIGRATION.md`.

---

## Credits

- Original project: [kfastov/telegram-mcp-server](https://github.com/kfastov/telegram-mcp-server) by **Konstantin Fastov** — MIT License
- This fork adds native stdio MCP transport by **Tharindu Mendis**
