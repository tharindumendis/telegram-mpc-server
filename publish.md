# 1. Login to npm
npm login

# 2. Git commit your changes first
git add -A
git commit -m "feat: add native stdio MCP transport (mcp-stdio.js + tgcli mcp --transport stdio)"
git push origin main

# 3. Publish (public flag required for scoped packages)
npm publish --access public
