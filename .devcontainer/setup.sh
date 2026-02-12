#!/bin/bash
set -e

echo "Starting setup..."

# Ensure vscode user owns workspace folders, to avoid git unsafe repo warnings
sudo chown -R vscode /workspaces/*

# Install uv
echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
# Ensure uv is in path for subsequent commands if needed (though it might need shell restart)
export PATH="/home/vscode/.cargo/bin:$PATH"

# Install Gemini CLI
echo "Installing Gemini CLI..."
# Assuming Gemini CLI installation via pip or npm as per requirement "via script"
# If there is a specific 'gemini-cli' package:
# pip install gemini-cli || echo "Warning: pip install gemini-cli failed."
# Or if it's via npm:
npm install -g @google/gemini-cli || echo "Warning: npm install @google/gemini-cli failed. Please verify package name."

# Install Claude Code CLI and TypeScript if not covered by features
# Requirement said "via dev container features", but if they are missing there, we can add here.
# However, I will stick to the requirement that they are features. 
# If they are not standard features, they might be community features or require this script.
# For now, assuming Claude Code CLI is 'claude' via npm if the feature fails/doesn't exist.
curl -fsSL https://claude.ai/install.sh | bash || echo "Warning: npm install @anthropic-ai/claude-code failed."
# Create symlink in /usr/local/bin so claude is available immediately for all shells (including PowerShell)
sudo ln -sf "$HOME/.local/bin/claude" /usr/local/bin/claude

# Install pyright for Python language server support (Claude Code plugin)
echo "Installing pyright..."
npm install -g pyright || echo "Warning: npm install pyright failed."

# Install Playwright MCP for browser automation
echo "Installing Playwright MCP..."
npm install -g @playwright/mcp@latest || echo "Warning: npm install @playwright/mcp failed."
# Install Playwright browsers (Firefox for better SSL handling in containers)
echo "Installing Playwright browsers..."
npx -y playwright install firefox || echo "Warning: Playwright browser installation failed."
# Install csharp-ls for C# language server support
echo "Installing csharp-ls..."
dotnet tool install --global csharp-ls || echo "Warning: dotnet tool install csharp-ls failed."
# Install Aspire CLI for MCP server support
echo "Installing Aspire CLI..."
dotnet tool install --global aspire.cli || echo "Warning: Aspire CLI installation failed."
# Setup .NET dev certificates for HTTPS (workaround for Aspire issue #13801)
# On Linux, SSL_CERT_DIR must point to the trust directory for partial trust to work
# See: https://github.com/dotnet/aspire/issues/13801
echo "Setting up .NET dev certificates..."
mkdir -p "$HOME/.aspnet/dev-certs/trust"
# Generate dev certificate if it doesn't exist
dotnet dev-certs https --check > /dev/null 2>&1 || dotnet dev-certs https
# Export the certificate to the trust directory for Linux partial trust
# This allows the Aspire dashboard and other services to trust the dev cert
dotnet dev-certs https --export-path "$HOME/.aspnet/dev-certs/trust/aspnetcore-https.crt" --format PEM --no-password 2>/dev/null || true
# Add SSL_CERT_DIR to shell profile for persistence across terminal sessions
if ! grep -q "SSL_CERT_DIR" "$HOME/.bashrc" 2>/dev/null; then
    echo 'export SSL_CERT_DIR="$HOME/.aspnet/dev-certs/trust"' >> "$HOME/.bashrc"
fi
if ! grep -q "SSL_CERT_DIR" "$HOME/.zshrc" 2>/dev/null; then
    echo 'export SSL_CERT_DIR="$HOME/.aspnet/dev-certs/trust"' >> "$HOME/.zshrc"
fi
echo "Dev certificates configured. SSL_CERT_DIR set to ~/.aspnet/dev-certs/trust"

# Install vendir CLI tool
echo "Installing vendir..."
wget https://github.com/carvel-dev/vendir/releases/download/v0.45.0/vendir-linux-amd64 -O ~/.local/bin/vendir
chmod +x ~/.local/bin/vendir

echo "Setup complete."

cat << EOF

**********************

TODO:
1. Review devcontainer.json to enable the required coding language-specific extensions for .NET, Python or NodeJS

GETTING STARTED TIPS:

Custom terminal profiles have been added for convenience. They can be accessed via the terminal dropdown menu.
They pre-configure the environment for using the installed CLIs with relaxed permission settings:

Claude Code        : claude --dangerously-skip-permissions
GitHub Copilot CLI : copilot --allow-all-tools
Gemini CLI         : gemini --approval-mode yolo

**********************

EOF
