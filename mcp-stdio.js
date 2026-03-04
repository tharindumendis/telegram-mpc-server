#!/usr/bin/env node
/**
 * mcp-stdio.js — tgcli MCP server over stdio
 * -------------------------------------------
 * Invoked by:  tgcli mcp --transport stdio
 *
 * Credentials are resolved in this priority order:
 *   1. TELEGRAM_API_ID / TELEGRAM_API_HASH environment variables
 *   2. Existing tgcli config file (written by `tgcli auth`)
 *
 * phoneNumber is only needed for interactive auth (`tgcli auth`).
 * For stdio MCP mode the session file written by a prior `tgcli auth`
 * run is sufficient — the MTProto client resumes from the stored session.
 *
 * Usage in Agent config (config.yaml):
 *   mcp_clients:
 *     - name: "telegram"
 *       command: "tgcli"
 *       args: ["mcp", "--transport", "stdio"]
 *       env:
 *         TELEGRAM_API_ID: "your_id"
 *         TELEGRAM_API_HASH: "your_hash"
 */

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig, normalizeConfig } from "./core/config.js";
import { createServices } from "./core/services.js";
import { resolveStoreDir } from "./core/store.js";
import { createServerInstance } from "./mcp-server.js";

// ── Resolve credentials ────────────────────────────────────────────────────
const storeDir = resolveStoreDir();

/**
 * Build a config object that merges the tgcli config file with env-var
 * overrides so the user can supply credentials without editing tgcli config.
 */
function resolveConfig() {
  const { config: fileConfig } = loadConfig(storeDir);
  const base = fileConfig ?? {};

  // Env vars override the config file
  const apiId = process.env.TELEGRAM_API_ID ?? base.apiId ?? null;
  const apiHash = process.env.TELEGRAM_API_HASH ?? base.apiHash ?? null;

  if (!apiId || !apiHash) {
    const missing = [];
    if (!apiId) missing.push("TELEGRAM_API_ID");
    if (!apiHash) missing.push("TELEGRAM_API_HASH");
    process.stderr.write(
      `[mcp-stdio] Missing required credentials: ${missing.join(", ")}.\n` +
        `  Set them as environment variables or run 'tgcli auth' first.\n`,
    );
    process.exit(1);
  }

  return normalizeConfig({ ...base, apiId, apiHash });
}

const config = resolveConfig();
const { telegramClient, messageSyncService } = createServices({
  storeDir,
  config,
});

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  process.stderr.write("[mcp-stdio] Connecting to Telegram…\n");
  try {
    await telegramClient.ensureLogin();
    await telegramClient.initializeDialogCache();
    await messageSyncService.refreshChannelsFromDialogs();
    messageSyncService.startRealtimeSync();
    messageSyncService.resumePendingJobs();
    process.stderr.write("[mcp-stdio] Telegram ready.\n");
  } catch (err) {
    // Non-fatal — individual tool calls will trigger ensureLogin()
    process.stderr.write(
      `[mcp-stdio] Warning: pre-warm failed (${err?.message ?? err}). Continuing.\n`,
    );
  }

  // createServerInstance() from mcp-server.js accepts injected services
  // so it uses OUR telegramClient/messageSyncService, not the HTTP-mode ones.
  const server = createServerInstance({ telegramClient, messageSyncService });
  const transport = new StdioServerTransport();

  await server.connect(transport);
  process.stderr.write("[mcp-stdio] MCP server ready on stdio.\n");

  async function shutdown() {
    process.stderr.write("[mcp-stdio] Shutting down…\n");
    try {
      await messageSyncService.shutdown();
    } catch (_) {
      /* ignore */
    }
    try {
      await telegramClient.destroy();
    } catch (_) {
      /* ignore */
    }
    process.exit(0);
  }

  process.once("SIGINT", () => void shutdown());
  process.once("SIGTERM", () => void shutdown());
}

main().catch((err) => {
  process.stderr.write(`[mcp-stdio] Fatal: ${err?.message ?? err}\n`);
  process.exit(1);
});
